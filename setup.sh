#!/bin/bash

set -e

# Always run from the project root (the folder containing this script), so the
# generated MCP config and venv paths are correct no matter where it is invoked from.
cd "$(dirname "$0")"

# Configuration
PROJECT_DIR="$(pwd -P)"
VENV_DIR="$PROJECT_DIR/venv"
AIRFLOW_VENV_DIR="$PROJECT_DIR/venv-airflow"   # separate env for local DagBag validation (see below)
SETUP_ERRORS=()   # problems that block the pipeline; reported at the end (exit 1)

echo "=========================================="
echo "HiveQL & Impala to Spark SQL Converter Setup"
echo "=========================================="

# Check Python
echo "🐍 Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    echo ""
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "💡 Install Python 3:"
        echo "   brew install python3"
        echo ""
        read -p "   Install Python 3 automatically? (y/n) " -n 1 -r || REPLY=n   # no TTY -> treat as "no"
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if command -v brew &> /dev/null; then
                brew install python3
                echo "✓ Python 3 installed"
            else
                # Deliberately not auto-installing Homebrew (that would pipe a
                # downloaded script straight into a shell). Install it yourself
                # from the official site, review it, then re-run this script.
                echo "❌ Homebrew not found."
                echo "   Install Homebrew from https://brew.sh, then re-run ./setup.sh"
                exit 1
            fi
        else
            echo "❌ Python 3 is required. Please install and run setup again."
            exit 1
        fi
    else
        echo "💡 Install Python 3:"
        echo "   Ubuntu/Debian: sudo apt-get install python3 python3-pip python3-venv"
        echo "   RHEL/CentOS: sudo yum install python3 python3-pip"
        exit 1
    fi
fi

# Pick the interpreter for the virtual environments.
# Prefer 3.12 / 3.11 / 3.10: these are the versions Apache Airflow publishes
# constraints for, so the optional DagBag validation env can be installed.
# Falls back to python3 if it is >= 3.10. Override with: PYTHON_BIN=/path/to/python ./setup.sh
if [ -z "${PYTHON_BIN:-}" ]; then
    for candidate in python3.12 python3.11 python3.10 python3; do
        if command -v "$candidate" &> /dev/null && \
           "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
            PYTHON_BIN="$(command -v "$candidate")"
            break
        fi
    done
fi
if [ -z "${PYTHON_BIN:-}" ]; then
    echo "❌ Python 3.10 or newer is required (3.10-3.12 recommended)."
    echo "   macOS: brew install python@3.12"
    exit 1
fi

# Verify Python version
PYTHON_VERSION=$("$PYTHON_BIN" --version 2>&1 | awk '{print $2}')
echo "✓ Python $PYTHON_VERSION found ($PYTHON_BIN)"

# Check pip
echo "📦 Checking pip..."
if ! "$PYTHON_BIN" -m pip --version &> /dev/null; then
    echo "⚠️  pip not found, installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        "$PYTHON_BIN" -m ensurepip --upgrade
    else
        echo "💡 Install pip:"
        echo "   Ubuntu/Debian: sudo apt-get install python3-pip"
        echo "   RHEL/CentOS: sudo yum install python3-pip"
        exit 1
    fi
fi
echo "✓ pip found"

# Create directory structure
#   input/   <- everything you provide
#   output/  <- everything that is generated
echo "📁 Creating directory structure..."
mkdir -p input/hive                 # HiveQL scripts (*.hql, *.txt)
mkdir -p input/impala                 # Impala SQL scripts (*.sql)
mkdir -p input/mwaa/control_m         # Control-M XML exports
mkdir -p input/mwaa/airflow           # On-prem Airflow DAGs (*.py)
mkdir -p input/mwaa/hive_scripts    # (optional) .hql scripts referenced by Control-M jobs
mkdir -p input/ddl                    # (optional) DDL files for schema inference in testing
for dialect in hive impala; do
    mkdir -p output/$dialect/spark_sql output/$dialect/chunks output/$dialect/mermaid
    mkdir -p output/$dialect/accuracy output/$dialect/validation
done
mkdir -p output/mwaa/job_ir output/mwaa/chunks output/mwaa/plans output/mwaa/dags
mkdir -p output/mwaa/validation output/mwaa/mermaid output/mwaa/reports
mkdir -p output/testing/generated_data
mkdir -p output/testing/execution_output/hive output/testing/execution_output/spark
mkdir -p output/testing/reports output/testing/state
mkdir -p output/state output/logs
echo "✓ Directory structure created (input/ and output/)"

# Create virtual environment
if [ -d "$VENV_DIR" ]; then
    # Check if existing venv matches current Python version
    VENV_PYTHON_VERSION=$("$VENV_DIR/bin/python3" --version 2>/dev/null | awk '{print $2}' || echo "none")
    SYSTEM_PYTHON_VERSION=$("$PYTHON_BIN" --version 2>&1 | awk '{print $2}')
    
    if [ "$VENV_PYTHON_VERSION" != "$SYSTEM_PYTHON_VERSION" ]; then
        echo "⚠️  Venv Python ($VENV_PYTHON_VERSION) differs from selected Python ($SYSTEM_PYTHON_VERSION)"
        echo "   Recreating virtual environments..."
        rm -rf "$VENV_DIR" "$AIRFLOW_VENV_DIR"
        "$PYTHON_BIN" -m venv "$VENV_DIR"
        echo "✓ Virtual environment recreated with Python $SYSTEM_PYTHON_VERSION"
    else
        echo "🐍 Virtual environment already exists (Python $VENV_PYTHON_VERSION)"
    fi
else
    echo "🐍 Creating virtual environment..."
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    echo "✓ Virtual environment created"
fi

# Use venv pip directly (more reliable than source activate in scripts)
PIP="$VENV_DIR/bin/pip"
VENV_PYTHON="$VENV_DIR/bin/python"

# Verify venv pip exists
if [ ! -f "$PIP" ]; then
    echo "❌ Error: $PIP not found after venv creation"
    echo "   Try: rm -rf venv && python3 -m venv venv"
    exit 1
fi

# Install dependencies. Everything is pinned to exact, tested versions:
#   - runtime packages come from requirements.txt (venv/)
#   - Apache Airflow (optional) is installed under the official Airflow constraints file (venv-airflow/)
echo "📦 Installing Python dependencies..."
"$PIP" install "pip==26.1.2" --quiet

PYTHON_VERSION_SHORT=$("$VENV_PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MINOR=$("$VENV_PYTHON" -c "import sys; print(sys.version_info.minor)")
if [ "$PYTHON_MINOR" -lt 10 ]; then
    echo "❌ Python $PYTHON_VERSION_SHORT is too old: the pinned dependencies (fastmcp) require Python 3.10 or newer."
    exit 1
fi

# Step 1: pinned runtime dependencies (the converter itself).
echo "  Installing pinned runtime dependencies..."
"$PIP" install -r requirements.txt --quiet
echo "✓ Runtime dependencies installed"

# Step 2: Apache Airflow in its OWN virtual environment (venv-airflow/). Optional: it is
# only used to validate generated DAGs locally (DagBag), run as a subprocess via
# validation.dagbag_python. Keeping it separate means Airflow's strict dependency
# constraints never conflict with the converter's pinned packages. Airflow publishes
# constraints per Python version, so the Airflow version is chosen to match; there is
# deliberately no unconstrained fallback.
echo "📦 Installing Apache Airflow (optional, for local DagBag validation of generated DAGs)..."
case "$PYTHON_MINOR" in
    10|11|12) AIRFLOW_VERSION="2.11.2" ;;
    *)     AIRFLOW_VERSION="" ;;
esac

AIRFLOW_INSTALLED=false
if [ -n "$AIRFLOW_VERSION" ]; then
    CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION_SHORT}.txt"
    if curl --output /dev/null --silent --head --fail "$CONSTRAINTS_URL"; then
        [ -d "$AIRFLOW_VENV_DIR" ] || "$VENV_PYTHON" -m venv "$AIRFLOW_VENV_DIR"
        AIRFLOW_PIP="$AIRFLOW_VENV_DIR/bin/pip"
        # The Amazon provider is installed under the same constraints so its version is pinned too.
        if "$AIRFLOW_PIP" install "pip==26.1.2" --quiet && \
           "$AIRFLOW_PIP" install "apache-airflow==${AIRFLOW_VERSION}" "apache-airflow-providers-amazon" \
               --constraint "${CONSTRAINTS_URL}" --quiet; then
            AIRFLOW_INSTALLED=true
        else
            echo "  ⚠  Airflow installation failed"
        fi
    else
        echo "  ⚠  Airflow ${AIRFLOW_VERSION} constraints file not reachable: ${CONSTRAINTS_URL}"
    fi
fi
if [ "$AIRFLOW_INSTALLED" = true ]; then
    echo "✓ Apache Airflow ${AIRFLOW_VERSION} installed in venv-airflow/"
else
    echo "  ⚠  Skipping Apache Airflow (no supported, constrained install for Python $PYTHON_VERSION_SHORT,"
    echo "     constraints unreachable, or install failed). Generated DAGs are still produced and"
    echo "     syntax-checked; the DagBag load check will be reported as skipped."
    echo "     Use Python 3.10-3.12 to enable it."
fi

# Create .env file
echo "⚙️  Creating .env file..."
cat > .env << EOF
# HiveQL to Spark SQL Converter Configuration
# All inputs live in input/, all generated files in output/ (see project_paths.py)
PROJECT_ROOT=$(pwd)
INPUT_DIR=input
OUTPUT_DIR=output

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_APP_NAME=HiveQLConverter
EOF
echo "✓ .env file created"

# Start (or initialise) the Finch VM without aborting setup on failure.
# Starting the VM needs sudo for its socket_vmnet network daemon; in a
# non-interactive shell (CI, sandboxed agents) that fails. Everything else in
# this script (venv, MCP config) does not depend on Finch, so a failure here is
# reported and the container steps are skipped instead of exiting.
FINCH_READY=false
ensure_finch_vm() {
    local status
    status=$(finch vm status 2>&1 || echo "not_initialized")
    if echo "$status" | grep -qi "running"; then
        echo "✓ Finch VM is already running"
    elif echo "$status" | grep -qi "not_initialized\|does not exist\|nonexistent"; then
        echo "⚙️  Initializing Finch VM (first time setup, about a minute)..."
        finch vm init || return 1
    else
        echo "🚀 Starting Finch VM..."
        finch vm start || return 1
    fi
    # Wait (up to ~90s) until the container engine inside the VM answers.
    echo "⏳ Waiting for Finch to be ready..."
    for _ in $(seq 1 30); do
        finch info &> /dev/null && return 0
        sleep 3
    done
    return 1
}

finch_vm_help() {
    echo "⚠️  Finch VM could not be started from this shell."
    echo "   Finch needs sudo to start its network daemon. Run in a normal terminal:"
    echo "      finch vm start        # enter your password if prompted"
    echo "      ./setup.sh            # re-run to pull images and start containers"
    echo "   Conversion MCP servers work without Finch; only Spark execution and"
    echo "   Hive/Spark equivalence testing need it."
}

# Check for Finch
echo "🔍 Checking for Finch..."
if command -v finch &> /dev/null; then
    echo "✓ Finch is installed"
    if ensure_finch_vm; then
        FINCH_READY=true
        echo "✓ Finch VM is ready"
    else
        finch_vm_help
    fi
else
    echo "⚠️  Finch not found"
    
    # Auto-install on macOS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo ""
        echo "💡 Finch is recommended for validation (optional)"
        echo ""
        read -p "   Install Finch automatically? (y/n) " -n 1 -r || REPLY=n   # no TTY -> treat as "no"
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "📦 Installing Finch via Homebrew..."
            
            # Check if brew is installed
            if command -v brew &> /dev/null; then
                brew install --cask finch
                
                if command -v finch &> /dev/null; then
                    echo "✓ Finch installed successfully"
                    if ensure_finch_vm; then
                        FINCH_READY=true
                        echo "✓ Finch VM initialized and ready"
                    else
                        finch_vm_help
                    fi
                else
                    echo "⚠️  Finch installation failed"
                fi
            else
                echo "⚠️  Homebrew not found"
                echo "   Install Homebrew first: https://brew.sh"
                echo "   Then run: brew install --cask finch"
            fi
        else
            echo "   Skipping Finch installation"
            echo "   You can install later: brew install --cask finch"
        fi
    else
        echo ""
        echo "💡 Finch is OPTIONAL but recommended for validation"
        echo "   To install:"
        echo "   Linux: See https://github.com/runfinch/finch"
        echo ""
        echo "   After installation, run: finch vm init"
    fi
    echo ""
fi

# Configure MCP servers
echo "🔧 Configuring MCP servers..."

# Check if .kiro/settings directory exists
WORKSPACE_MCP_CONFIG="$PROJECT_DIR/.kiro/settings/mcp.json"
KIRO_SETTINGS_DIR="$(dirname "$WORKSPACE_MCP_CONFIG")"

# Create .kiro/settings directory if it doesn't exist
if [ ! -d "$KIRO_SETTINGS_DIR" ]; then
    echo "📁 Creating .kiro/settings directory (new user setup)..."
    mkdir -p "$KIRO_SETTINGS_DIR"
    echo "✓ Directory created: $KIRO_SETTINGS_DIR"
fi

# Backup existing config if present
if [ -f "$WORKSPACE_MCP_CONFIG" ]; then
    BACKUP_FILE="$WORKSPACE_MCP_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    echo "📄 Backing up existing MCP config to: $(basename "$BACKUP_FILE")"
    cp "$WORKSPACE_MCP_CONFIG" "$BACKUP_FILE"
else
    echo "📝 Creating new MCP configuration (first time setup)..."
fi

VENV_PYTHON="$VENV_DIR/bin/python"

cat > "$WORKSPACE_MCP_CONFIG" << EOF
{
  "mcpServers": {
    "hive-converter": {
      "command": "$VENV_PYTHON",
      "args": [
        "src/mcp_servers/hive_converter_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "disabled": false,
      "autoApprove": [
        "identify_hive_file",
        "chunk_hive_file",
        "create_detailed_mermaid",
        "generate_spark_sql",
        "merge_spark_sql_chunks",
        "validate_spark_sql",
        "fix_conversion_errors"
      ]
    },
    "hive-eval": {
      "command": "$VENV_PYTHON",
      "args": [
        "src/mcp_servers/hive_eval_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "disabled": false,
      "autoApprove": [
        "execute_spark_sql"
      ]
    },
    "mwaa-converter": {
      "command": "$VENV_PYTHON",
      "args": [
        "mwaa-converter/converter/mcp/mwaa_converter_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "env": {
        "PYTHONPATH": "$PROJECT_DIR/mwaa-converter"
      },
      "disabled": false,
      "autoApprove": [
        "parse_control_m_job",
        "validate_parsed_job_ir",
        "parse_airflow_dag",
        "validate_parsed_airflow_job_ir",
        "generate_conversion_plan",
        "validate_conversion_plan",
        "generate_dag_code",
        "create_orchestration_mermaid",
        "merge_conversion_reports",
        "merge_airflow_variables"
      ]
    },
    "mwaa-validator": {
      "command": "$VENV_PYTHON",
      "args": [
        "mwaa-converter/converter/mcp/mwaa_validator_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "env": {
        "PYTHONPATH": "$PROJECT_DIR/mwaa-converter"
      },
      "disabled": false,
      "autoApprove": [
        "validate_syntax",
        "validate_structure",
        "validate_security",
        "validate_dagbag",
        "fix_validation_errors"
      ]
    },
    "accuracy-calculator": {
      "command": "$VENV_PYTHON",
      "args": [
        "accuracy_framework/mcp/accuracy_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "disabled": false,
      "autoApprove": [
        "analyze_accuracy",
        "generate_accuracy_report",
        "generate_dashboard"
      ]
    },
    "test-orchestrator": {
      "command": "$VENV_PYTHON",
      "args": [
        "testing_framework/src/mcp_servers/test_orchestrator_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "env": {
        "PYTHONPATH": "$PROJECT_DIR/testing_framework/src"
      },
      "disabled": false,
      "autoApprove": [
        "extract_or_load_ddl",
        "generate_test_data",
        "analyze_dependencies",
        "compare_results",
        "run_minus_validation",
        "generate_test_report"
      ]
    },
    "test-execution": {
      "command": "$VENV_PYTHON",
      "args": [
        "testing_framework/src/mcp_servers/test_execution_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "env": {
        "PYTHONPATH": "$PROJECT_DIR/testing_framework/src"
      },
      "disabled": false,
      "autoApprove": [
        "start_test_containers",
        "load_test_data_hive",
        "load_test_data_spark",
        "execute_hive_query",
        "execute_spark_query",
        "stop_test_containers"
      ]
    },
    "impala-converter": {
      "command": "$VENV_PYTHON",
      "args": [
        "src/mcp_servers/impala_converter_server.py"
      ],
      "cwd": "$PROJECT_DIR",
      "disabled": false,
      "autoApprove": [
        "identify_impala_file",
        "generate_spark_sql_from_impala",
        "validate_impala_spark_sql",
        "fix_impala_conversion_errors",
        "create_impala_mermaid",
        "chunk_impala_file",
        "merge_impala_spark_chunks"
      ]
    }
  }
}
EOF

echo "✓ MCP configuration created at: .kiro/settings/mcp.json"

# Prevent stale/duplicate project servers in the GLOBAL Kiro config.
#
# These MCP servers belong to this project and are defined in the workspace
# config above. If a previous setup.sh run (possibly from a different checkout)
# also wrote them into the user-level ~/.kiro/settings/mcp.json, the duplicate
# server names collide across scopes on load. The stale global copies point at
# the old checkout's paths and fail to connect, which looks like "some MCP
# servers failed after setup.sh". We remove any project-owned entries from the
# global config so the workspace config is the single source of truth.
GLOBAL_MCP_CONFIG="$HOME/.kiro/settings/mcp.json"
if [ -f "$GLOBAL_MCP_CONFIG" ]; then
    echo ""
    echo "🧹 Checking global MCP config for duplicate project servers..."
    GLOBAL_BACKUP="$GLOBAL_MCP_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$GLOBAL_MCP_CONFIG" "$GLOBAL_BACKUP"
    CLEANUP_OUTPUT=$(python3 - "$GLOBAL_MCP_CONFIG" "$GLOBAL_BACKUP" <<'PYEOF'
import json, sys, os

global_path, backup_path = sys.argv[1], sys.argv[2]

# Project-owned server names (must match the workspace config written above).
PROJECT_SERVERS = [
    "hive-converter",
    "hive-eval",
    "hiveql-converter",   # legacy names (renamed) -- remove stale entries too
    "hiveql-eval",
    "mwaa-converter",
    "mwaa-validator",
    "accuracy-calculator",
    "test-orchestrator",
    "test-execution",
    "impala-converter",
]

try:
    with open(global_path) as f:
        data = json.load(f)
except (ValueError, OSError) as e:
    print("SKIP: could not parse global config (%s); leaving it untouched" % e)
    sys.exit(0)

servers = data.get("mcpServers")
if not isinstance(servers, dict):
    print("SKIP: no mcpServers object in global config; nothing to clean")
    sys.exit(0)

removed = [name for name in PROJECT_SERVERS if name in servers]
for name in removed:
    del servers[name]

if not removed:
    # No changes needed; drop the just-created backup to avoid clutter.
    try:
        os.remove(backup_path)
    except OSError:
        pass
    print("OK: no duplicate project servers in global config")
    sys.exit(0)

with open(global_path, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print("REMOVED: %s" % ", ".join(removed))
print("KEPT: %s" % (", ".join(servers.keys()) if servers else "(none)"))
PYEOF
)
    echo "$CLEANUP_OUTPUT" | sed 's/^/   /'
    if echo "$CLEANUP_OUTPUT" | grep -q '^REMOVED:'; then
        echo "   📄 Global config backed up to: $(basename "$GLOBAL_BACKUP")"
    fi
fi

echo ""
echo "📋 MCP Servers Configured:"
echo "   • hive-converter (7 tools) - HiveQL → Spark SQL conversion"
echo "   • hive-eval (1 tool) - Spark SQL execution in Finch"
echo "   • mwaa-converter (10 tools) - Control-M/Airflow → MWAA DAGs"
echo "   • mwaa-validator (5 tools) - DAG validation"
echo "   • accuracy-calculator (3 tools) - 8-dimension accuracy analysis"
echo "   • test-orchestrator (6 tools) - Hive/Spark equivalence testing"
echo "   • test-execution (6 tools) - Test container lifecycle & execution"
echo "   • impala-converter (7 tools) - Impala → Spark SQL conversion"
echo ""
echo "💡 MCP servers will be available in Kiro IDE"
echo "💡 Auto-approved tools: all 45 tools across 8 servers"

# Verify MCP configuration: start every server exactly as the orchestrator does
# (orchestrator/mcp_client.py) and list its tools. A file-exists check is not
# enough: a broken import or dependency only shows up when the server starts.
echo ""
echo "🔍 Verifying MCP servers (starting each one and listing its tools)..."
MCP_CHECK_LOG="$PROJECT_DIR/output/logs/setup_mcp_check.log"
if "$VENV_PYTHON" - "$WORKSPACE_MCP_CONFIG" 2> "$MCP_CHECK_LOG" <<'MCPCHECK'
import asyncio, sys
from orchestrator.mcp_client import MCPClientPool

async def main(path):
    pool = MCPClientPool.from_mcp_json(path)
    failed = 0
    for name in pool._configs:
        try:
            await asyncio.wait_for(pool.connect(name), 90)
            print("   ✓ %-20s %d tools" % (name, len(await pool.list_tools(name))))
        except Exception as e:  # report every server, not just the first failure
            failed += 1
            print("   ❌ %-20s failed to start: %s" % (name, e))
    await pool.close_all()
    return failed

sys.exit(1 if asyncio.run(main(sys.argv[1])) else 0)
MCPCHECK
then
    echo "✓ All MCP servers start and respond"
else
    echo "❌ One or more MCP servers failed to start (details: output/logs/setup_mcp_check.log)"
    SETUP_ERRORS+=("MCP servers failed to start (see output/logs/setup_mcp_check.log)")
fi

# The orchestrator hands prompt-returning tools to an LLM CLI (kiro-cli by
# default). It must be installed, logged in, and able to see these servers.
echo ""
echo "🔍 Checking the LLM backend (kiro-cli)..."
if command -v kiro-cli &> /dev/null; then
    if kiro-cli whoami &> /dev/null; then
        echo "✓ kiro-cli is installed and logged in"
        # kiro-cli prints this list on stderr, so capture both streams.
        if kiro-cli mcp list 2>&1 | grep -q "hive-converter"; then
            echo "✓ kiro-cli sees the project MCP servers"
        else
            echo "❌ kiro-cli does not list the project MCP servers"
            SETUP_ERRORS+=("kiro-cli cannot see .kiro/settings/mcp.json (run 'kiro-cli mcp list' in $PROJECT_DIR)")
        fi
    else
        echo "❌ kiro-cli is not logged in. Run: kiro-cli login"
        SETUP_ERRORS+=("kiro-cli is not logged in (run 'kiro-cli login', then re-run ./setup.sh)")
    fi
elif command -v claude &> /dev/null; then
    echo "⚠️  kiro-cli not found, but 'claude' is: add --backend claude to orchestrator commands"
else
    echo "❌ Neither kiro-cli nor claude is installed; the conversion steps need one of them"
    SETUP_ERRORS+=("no LLM CLI installed (kiro-cli or claude)")
fi

# requirements.txt is tracked in the repo (exact pins), so it is intentionally NOT regenerated here.

# Check for HiveQL input files
echo "📋 Checking HiveQL input files..."
FILE_COUNT=$(find input/hive -type f \( -name "*.hql" -o -name "*.txt" \) 2>/dev/null | wc -l | tr -d ' ')

if [ "$FILE_COUNT" -gt 0 ]; then
    echo "✓ Found $FILE_COUNT HiveQL file(s)"
    find input/hive -type f \( -name "*.hql" -o -name "*.txt" \) 2>/dev/null | while read file; do
        echo "    - $(basename "$file")"
    done
else
    echo "⚠️  No HiveQL files found in input/hive/"
    echo "   Add files: cp /path/to/*.hql input/hive/"
    echo "   Or: cp /path/to/*.txt input/hive/"
fi

# Containers (only if the Finch VM came up earlier)
#   converter-spark  long-running Spark used by hive-eval to execute converted SQL
#   apache/hive      pre-pulled only; the testing framework starts its own
#                    hive-test-runner / spark-test-runner via finch-compose-test.yml
SPARK_IMAGE="apache/spark:3.5.0"
HIVE_IMAGE="apache/hive:4.0.0"
SPARK_READY=false

# Pull an image unless it is already present locally.
ensure_image() {
    local image="$1"
    if finch image inspect "$image" &> /dev/null; then
        echo "   ✓ Image $image present"
        return 0
    fi
    echo "   📥 Pulling $image (first time only, can take a few minutes)..."
    if finch pull "$image"; then
        echo "   ✓ Pulled $image"
    else
        echo "   ❌ Failed to pull $image (check network / registry access)"
        return 1
    fi
}

container_exists() {
    finch ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$1"
}

if [ "$FINCH_READY" = true ]; then
    echo ""
    echo "🐳 Setting up containers..."

    # Remove containers from older versions of this script:
    #   hiveql-spark / hiveql-hive  old names (held ports 4040, 8080, 10000)
    #   converter-hive              held 127.0.0.1:10000, which the testing
    #                               framework's hive-test-runner needs
    for legacy in hiveql-spark hiveql-hive converter-hive; do
        if container_exists "$legacy"; then
            echo "   🧹 Removing old container $legacy..."
            finch rm -f "$legacy" > /dev/null 2>&1 || true
        fi
    done

    IMAGES_OK=true
    ensure_image "$SPARK_IMAGE" || IMAGES_OK=false
    ensure_image "$HIVE_IMAGE"  || IMAGES_OK=false
    if [ "$IMAGES_OK" != true ]; then
        SETUP_ERRORS+=("could not pull container images ($SPARK_IMAGE / $HIVE_IMAGE)")
    fi

    # Recreate converter-spark so it always uses the current mounts and settings.
    if container_exists converter-spark; then
        echo "   🧹 Removing existing converter-spark container..."
        finch rm -f converter-spark > /dev/null 2>&1 || true
    fi

    echo "   🚀 Starting converter-spark..."
    # Same hardening as finch-compose.yml: localhost-only ports, read-only root with
    # tmpfs for Spark's write paths, no capabilities, no-new-privileges, resource limits.
    # SPARK_LOCAL_IP / SPARK_LOCAL_HOSTNAME: outside a compose network Finch does not
    # put the container's own hostname in /etc/hosts, and Spark aborts at start-up
    # with UnknownHostException unless it is told to bind to localhost.
    if finch run -d \
        --name converter-spark \
        -p 127.0.0.1:4040:4040 \
        -p 127.0.0.1:8080:8080 \
        --read-only \
        --tmpfs /tmp:exec,mode=1777,size=1g \
        --tmpfs /opt/spark/work-dir:uid=185,gid=185,mode=0755,size=1g \
        -w /opt/spark/work-dir \
        --cap-drop ALL \
        --security-opt no-new-privileges \
        --memory 4g --cpus 2 \
        -e SPARK_LOCAL_IP=127.0.0.1 \
        -e SPARK_LOCAL_HOSTNAME=localhost \
        -v "$PROJECT_DIR/output/hive/spark_sql:/workspace/hive:ro" \
        -v "$PROJECT_DIR/output/impala/spark_sql:/workspace/impala:ro" \
        "$SPARK_IMAGE" \
        tail -f /dev/null > /dev/null; then

        # Readiness = Spark can actually run a query, not just "container is up".
        echo "   ⏳ Checking that Spark SQL runs inside the container..."
        if finch exec converter-spark /opt/spark/bin/spark-sql --master 'local[*]' \
               -e "SELECT 'spark_ok'" 2>/dev/null | grep -q spark_ok; then
            SPARK_READY=true
            echo "   ✓ converter-spark is running and executes Spark SQL"
            echo "     Converted SQL is mounted at /workspace/hive and /workspace/impala"
        else
            echo "   ❌ converter-spark started but Spark SQL failed. Logs: finch logs converter-spark"
            SETUP_ERRORS+=("Spark SQL does not run in converter-spark (finch logs converter-spark)")
        fi
    else
        echo "   ❌ Failed to create converter-spark"
        echo "      Check: finch vm status | lsof -i :4040 | finch rm -f converter-spark"
        SETUP_ERRORS+=("could not create the converter-spark container")
    fi

    # Port 10000 must be free for the testing framework's Hive container.
    if command -v lsof &> /dev/null && lsof -nP -iTCP:10000 -sTCP:LISTEN &> /dev/null; then
        echo "   ⚠️  Port 10000 is in use; --intent test_only needs it for hive-test-runner:"
        lsof -nP -iTCP:10000 -sTCP:LISTEN | sed 's/^/      /'
    fi
else
    echo ""
    if command -v finch &> /dev/null; then
        echo "⏭️  Skipping containers (Finch VM not running)."
        echo "   After 'finch vm start' in a normal terminal, re-run ./setup.sh to create them."
        SETUP_ERRORS+=("Finch VM is not running (run 'finch vm start', then ./setup.sh again)")
    else
        echo "⏭️  Skipping containers (Finch is not installed)."
        SETUP_ERRORS+=("Finch is not installed (brew install --cask finch, then ./setup.sh again)")
    fi
fi

# Summary. (The venv cannot be activated for your shell from inside this script:
# activate it yourself, or call venv/bin/python directly.)
echo ""
echo "========================================="
if [ ${#SETUP_ERRORS[@]} -eq 0 ]; then
    echo "✅ Setup complete: everything is ready"
else
    echo "❌ Setup finished with ${#SETUP_ERRORS[@]} problem(s):"
    for err in "${SETUP_ERRORS[@]}"; do
        echo "   • $err"
    done
fi
echo "========================================="
echo ""
echo "📍 Project:     $PROJECT_DIR"
echo "🐍 Python venv: venv/ (Airflow DagBag venv: venv-airflow/)"
echo "⚙️  MCP config:  .kiro/settings/mcp.json (8 servers)"
if [ "$SPARK_READY" = true ]; then
    echo "🐳 Spark:       converter-spark running (Spark UI http://localhost:4040)"
else
    echo "🐳 Spark:       NOT ready"
fi
echo ""
echo "📂 Put your files in input/:"
echo "   input/hive/          *.hql, *.txt   (found: $FILE_COUNT)"
echo "   input/impala/        *.sql"
echo "   input/mwaa/control_m *.xml          (or input/mwaa/airflow/*.py with --source-type airflow)"
echo ""
echo "🚀 Run everything (HiveQL + Impala + MWAA) with one command:"
echo "   source venv/bin/activate"
echo "   python -m orchestrator.orchestrator_agent --intent full_pipeline"
echo ""
echo "   Single pipelines: --intent hive_only | impala_only | mwaa_only | test_only"
echo "   Results land in output/ (e.g. output/hive/accuracy/dashboard.html)"
echo ""
echo "💡 Kiro IDE loads .kiro/settings/mcp.json from the folder you open as the"
echo "   workspace. Open this folder ($PROJECT_DIR) in Kiro"
echo "   to use the MCP tools interactively; the orchestrator does not need this."
echo ""

[ ${#SETUP_ERRORS[@]} -eq 0 ] || exit 1

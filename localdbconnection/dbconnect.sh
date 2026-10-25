#!/usr/bin/env bash
#
# dbconnect.sh — one command to connect to any of our AWS RDS PostgreSQL databases.
#
# It handles the whole flow for you in a single terminal:
#   1. makes sure you're logged in to AWS SSO
#   2. finds the EC2 bastion host
#   3. maps the RDS hostname to localhost (once)
#   4. opens the SSM port-forwarding tunnel in the background
#   5. generates a short-lived IAM auth token
#   6. opens a psql session
#   7. leaves the tunnel running for GUI clients and later commands
#
# Day-to-day usage (just run it and pick from the menu):
#   ./dbconnect.sh
#
# Or target a specific database directly:
#   ./dbconnect.sh connect orqis-dev     # open a psql session (default action)
#   ./dbconnect.sh tunnel  orqis-dev     # open the background tunnel (for pgAdmin/DBeaver)
#   ./dbconnect.sh token   orqis-dev     # open the tunnel and print settings for a GUI client
#   ./dbconnect.sh login   orqis-dev     # just (re)authenticate the SSO session
#   ./dbconnect.sh stop    orqis-dev     # stop a background tunnel
#   ./dbconnect.sh list                  # list configured databases
#   ./dbconnect.sh clean                 # remove RDS host entries from /etc/hosts
#
# All database definitions live in connections.json — add a new database or
# environment there and it shows up here automatically. No secrets are stored;
# IAM tokens are generated on demand and expire after 15 minutes.
#
set -euo pipefail

BOLD=$'\e[1m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; CYAN=$'\e[36m'; DIM=$'\e[2m'; RESET=$'\e[0m'
info()  { printf '%s\n' "${BOLD}==>${RESET} $*"; }
ok()    { printf '%s\n' "  ${GREEN}✓${RESET} $*"; }
warn()  { printf '%s\n' "  ${YELLOW}!${RESET} $*"; }
die()   { printf '%s\n' "  ${RED}✗${RESET} $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${SCRIPT_DIR}/connections.json"

command -v python3 >/dev/null 2>&1 || die "python3 is required (used to read connections.json)."
command -v aws     >/dev/null 2>&1 || die "aws CLI not found. Run ./bootstrap.sh first."
[ -f "$CONFIG" ]                   || die "Config not found: $CONFIG"

# --- config access ----------------------------------------------------------
# cfg_get <target> <field>  → prints the value, applying defaults where relevant.
cfg_get() {
  python3 - "$CONFIG" "$1" "$2" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
target, field = sys.argv[2], sys.argv[3]
t = cfg["targets"].get(target)
if t is None:
    sys.exit(f"Unknown target: {target}")
defaults = cfg.get("defaults", {})
val = t.get(field, defaults.get(field, ""))
print(val if val is not None else "")
PY
}

list_target_ids() {
  python3 - "$CONFIG" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
for name in cfg["targets"]:
    print(name)
PY
}

print_targets() {
  info "Configured databases:"
  python3 - "$CONFIG" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
for name, t in cfg["targets"].items():
    print(f"  {name:20s} {t.get('description','')}")
PY
}

require_target() {
  local target="$1"
  list_target_ids | grep -qx "$target" || die "Unknown target '$target'. Run './dbconnect.sh list'."
}

# --- interactive picker -----------------------------------------------------
pick_target() {
  # read loop instead of mapfile: macOS ships bash 3.2, which has no mapfile.
  local ids=() line
  while IFS= read -r line; do [ -n "$line" ] && ids+=("$line"); done < <(list_target_ids)
  [ "${#ids[@]}" -gt 0 ] || die "No targets defined in connections.json."
  if [ "${#ids[@]}" -eq 1 ]; then echo "${ids[0]}"; return; fi
  printf '%s\n' "${BOLD}Select a database:${RESET}" >&2
  local id
  select id in "${ids[@]}"; do
    [ -n "${id:-}" ] && { echo "$id"; return; }
    printf '%s\n' "Invalid choice, try again." >&2
  done
}

# --- AWS session ------------------------------------------------------------
ensure_login() {
  local profile="$1" region="$2"
  export AWS_PROFILE="$profile" AWS_REGION="$region" AWS_DEFAULT_REGION="$region"
  if aws sts get-caller-identity >/dev/null 2>&1; then
    ok "AWS SSO session active (profile: ${profile})"
  else
    info "Logging in to AWS SSO (profile: ${profile})"
    aws sso login --profile "$profile" --use-device-code \
      || die "SSO login failed. Is the profile configured? (aws configure sso --profile ${profile})"
    aws sts get-caller-identity >/dev/null 2>&1 || die "Still not authenticated after login."
    ok "Logged in (profile: ${profile})"
  fi
}

resolve_bastion() {
  local name="$1" iid
  iid="$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" --output text 2>/dev/null || true)"
  [ -n "$iid" ] && [ "$iid" != "None" ] || die "Could not find a running EC2 bastion named '${name}'."
  echo "$iid"
}

ensure_hosts_entry() {
  local host="$1"
  if grep -qE "^127\.0\.0\.1[[:space:]]+${host}(\$|[[:space:]])" /etc/hosts; then
    ok "/etc/hosts already maps ${host} → 127.0.0.1"
  else
    info "Adding /etc/hosts entry for ${host} (sudo may prompt)"
    echo "127.0.0.1 ${host}" | sudo tee -a /etc/hosts >/dev/null
    ok "/etc/hosts updated"
  fi
}

wait_for_port() {
  local port="$1" tries="${2:-30}"
  for _ in $(seq 1 "$tries"); do
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 1
  done
  return 1
}

get_tunnel_pid_file() {
  printf '%s/dbconnect-%s.tunnel.pid\n' "${TMPDIR:-/tmp}" "$1"
}

start_tunnel_bg() {
  local iid="$1" rdshost="$2" remote="$3" local_port="$4"
  local pid_file="$5" log_file="${pid_file%.pid}.log"
  nohup aws ssm start-session \
    --target "$iid" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "host=${rdshost},portNumber=${remote},localPortNumber=${local_port}" \
    </dev/null >"$log_file" 2>&1 &
  printf '%s\n' "$!" >"$pid_file"
}

ensure_tunnel() {
  local target="$1" iid="$2" rdshost="$3" remote="$4" local_port="$5"
  local pid_file; pid_file="$(get_tunnel_pid_file "$target")"
  local tunnel_pid=""
  [ -f "$pid_file" ] && read -r tunnel_pid <"$pid_file" || true

  if [ -n "$tunnel_pid" ] && kill -0 "$tunnel_pid" >/dev/null 2>&1 && wait_for_port "$local_port" 1; then
    ok "Tunnel already running on 127.0.0.1:${local_port} (PID ${tunnel_pid})"
    return 0
  fi

  if wait_for_port "$local_port" 1; then
    warn "Port ${local_port} is already in use; assuming the tunnel is running."
    return 0
  fi

  info "Starting background SSM tunnel  127.0.0.1:${local_port} → ${rdshost}:${remote}"
  start_tunnel_bg "$iid" "$rdshost" "$remote" "$local_port" "$pid_file"
  wait_for_port "$local_port" 30 || {
    warn "Tunnel log: ${pid_file%.pid}.log"
    die "Tunnel did not come up on port ${local_port}."
  }
  ok "Tunnel established and left running (PID $(cat "$pid_file"))"
}

# Generates an RDS IAM auth token using short-lived assumed-role credentials.
gen_token() {
  local role_arn="$1" region="$2" rdshost="$3" remote="$4" dbuser="$5"
  local creds ak sk st
  creds="$(aws sts assume-role \
    --role-arn "$role_arn" \
    --role-session-name "pg-iam-$(date +%Y%m%d%H%M%S)" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
    --output text)" || die "assume-role failed for ${role_arn}"
  read -r ak sk st <<<"$creds"
  AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" AWS_SESSION_TOKEN="$st" \
    aws rds generate-db-auth-token \
      --hostname "$rdshost" --port "$remote" --region "$region" --username "$dbuser" \
    || die "Failed to generate IAM auth token."
}

# --- commands ---------------------------------------------------------------
cmd_connect() {
  local target="$1"; require_target "$target"
  local profile region role bastion rdshost dbname dbuser local_port remote sslmode
  profile="$(cfg_get "$target" profile)";     region="$(cfg_get "$target" region)"
  role="$(cfg_get "$target" roleArn)";         bastion="$(cfg_get "$target" bastionName)"
  rdshost="$(cfg_get "$target" rdsHost)";      dbname="$(cfg_get "$target" dbName)"
  dbuser="$(cfg_get "$target" dbUser)";        local_port="$(cfg_get "$target" localPort)"
  remote="$(cfg_get "$target" remotePort)";    sslmode="$(cfg_get "$target" sslmode)"
  remote="${remote:-5432}"; sslmode="${sslmode:-require}"

  info "Connecting to ${CYAN}${target}${RESET} (${dbname} as ${dbuser})"
  ensure_login "$profile" "$region"
  local iid; iid="$(resolve_bastion "$bastion")"; ok "Bastion: ${bastion} (${iid})"
  ensure_hosts_entry "$rdshost"

  ensure_tunnel "$target" "$iid" "$rdshost" "$remote" "$local_port"

  info "Generating IAM auth token (valid 15 min)"
  local token; token="$(gen_token "$role" "$region" "$rdshost" "$remote" "$dbuser")"
  ok "Token acquired"

  info "Opening psql — type ${BOLD}\\q${RESET} to quit"
  PGPASSWORD="$token" PGSSLMODE="$sslmode" \
    psql "host=${rdshost} port=${local_port} dbname=${dbname} user=${dbuser} sslmode=${sslmode}" || true

  info "Session closed. Tunnel will be shut down."
}

cmd_tunnel() {
  local target="$1"; require_target "$target"
  local profile region bastion rdshost local_port remote
  profile="$(cfg_get "$target" profile)"; region="$(cfg_get "$target" region)"
  bastion="$(cfg_get "$target" bastionName)"; rdshost="$(cfg_get "$target" rdsHost)"
  local_port="$(cfg_get "$target" localPort)"; remote="$(cfg_get "$target" remotePort)"; remote="${remote:-5432}"

  info "Tunnel-only mode for ${CYAN}${target}${RESET}"
  ensure_login "$profile" "$region"
  local iid; iid="$(resolve_bastion "$bastion")"; ok "Bastion: ${bastion} (${iid})"
  ensure_hosts_entry "$rdshost"
  ensure_tunnel "$target" "$iid" "$rdshost" "$remote" "$local_port"
  warn "Tunnel is running in the background. Use './dbconnect.sh clean' only to remove host entries;"
  warn "to stop this tunnel, run:  ./dbconnect.sh stop ${target}"
}

cmd_token() {
  local target="$1"; require_target "$target"
  local profile region role bastion rdshost dbname dbuser local_port remote sslmode
  profile="$(cfg_get "$target" profile)"; region="$(cfg_get "$target" region)"
  bastion="$(cfg_get "$target" bastionName)"
  role="$(cfg_get "$target" roleArn)"; rdshost="$(cfg_get "$target" rdsHost)"
  dbname="$(cfg_get "$target" dbName)"; dbuser="$(cfg_get "$target" dbUser)"
  local_port="$(cfg_get "$target" localPort)"; remote="$(cfg_get "$target" remotePort)"; remote="${remote:-5432}"
  sslmode="$(cfg_get "$target" sslmode)"; sslmode="${sslmode:-require}"

  ensure_login "$profile" "$region" >&2
  local iid; iid="$(resolve_bastion "$bastion")"
  ensure_hosts_entry "$rdshost"
  ensure_tunnel "$target" "$iid" "$rdshost" "$remote" "$local_port"
  local token; token="$(gen_token "$role" "$region" "$rdshost" "$remote" "$dbuser")"
  cat <<EOF

${BOLD}Connection settings for a GUI client (pgAdmin / DBeaver):${RESET}
  Host     : ${rdshost}     ${DIM}(must be this exact hostname; tunnel must be running)${RESET}
  Port     : ${local_port}
  Database : ${dbname}
  Username : ${dbuser}
  SSL mode : ${sslmode}
  Password : (the token below — valid 15 minutes, regenerate when it expires)

${BOLD}Password / token:${RESET}
${token}

${DIM}The tunnel is running in the background on 127.0.0.1:${local_port}.${RESET}
EOF
}

cmd_stop() {
  local target="$1"; require_target "$target"
  local pid_file; pid_file="$(get_tunnel_pid_file "$target")"
  local tunnel_pid=""
  [ -f "$pid_file" ] && read -r tunnel_pid <"$pid_file" || true
  if [ -n "$tunnel_pid" ] && kill -0 "$tunnel_pid" >/dev/null 2>&1; then
    kill "$tunnel_pid" >/dev/null 2>&1 || true
    ok "Stopped tunnel for ${target} (PID ${tunnel_pid})"
  else
    warn "No running tunnel found for ${target}."
  fi
  rm -f "$pid_file"
}

cmd_login() {
  local target="$1"; require_target "$target"
  ensure_login "$(cfg_get "$target" profile)" "$(cfg_get "$target" region)"
}

cmd_clean() {
  local hosts=() line
  while IFS= read -r line; do [ -n "$line" ] && hosts+=("$line"); done < <(python3 - "$CONFIG" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
for t in cfg["targets"].values():
    h = t.get("rdsHost")
    if h: print(h)
PY
)
  info "Removing RDS host entries from /etc/hosts (sudo may prompt)"
  for h in "${hosts[@]}"; do
    if grep -qE "[[:space:]]${h}(\$|[[:space:]])" /etc/hosts; then
      sudo sed -i "/[[:space:]]${h//./\\.}\$/d;/[[:space:]]${h//./\\.}[[:space:]]/d" /etc/hosts
      ok "Removed ${h}"
    fi
  done
}

# --- entry point ------------------------------------------------------------
main() {
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    list)         print_targets ;;
    connect)      cmd_connect "${1:-$(pick_target)}" ;;
    tunnel)       cmd_tunnel  "${1:?usage: dbconnect.sh tunnel <target>}" ;;
    token)        cmd_token   "${1:-$(pick_target)}" ;;
    login)        cmd_login   "${1:?usage: dbconnect.sh login <target>}" ;;
    stop)         cmd_stop    "${1:?usage: dbconnect.sh stop <target>}" ;;
    clean)        cmd_clean ;;
    ""|menu)      cmd_connect "$(pick_target)" ;;
    -h|--help|help)
      sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
    *)
      # Allow "./dbconnect.sh orqis-dev" as shorthand for "connect orqis-dev".
      if list_target_ids | grep -qx "$cmd"; then cmd_connect "$cmd"
      else die "Unknown command '${cmd}'. Try './dbconnect.sh --help'."; fi ;;
  esac
}

main "$@"

# OPA Rego policy stub — M3 feature
# Inline rule evaluation is used in Phase 1 (opa_client._enabled = False).
# This file is loaded when OPA is enabled via OPA_URL env var in M3.

package acme.github.rls

import future.keywords.if

# Allow row if user's team_id matches the PR's team_id
allow if {
    input.user.team_id == input.row.team_id
}

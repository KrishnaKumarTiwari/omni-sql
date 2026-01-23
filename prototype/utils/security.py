import hashlib
from typing import Dict, Any, List

class SecurityEnforcer:
    @staticmethod
    def authenticate(user_token: str) -> Dict[str, Any]:
        """
        Simulated OIDC/AuthN. Returns user context.
        """
        users = {
            "token_dev": {"user_id": "u1", "role": "developer", "team_id": "mobile", "pii_access": True},
            "token_qa": {"user_id": "u2", "role": "qa", "team_id": "mobile", "pii_access": False},
            "token_web_dev": {"user_id": "u3", "role": "developer", "team_id": "web", "pii_access": True}
        }
        return users.get(user_token, {"user_id": "anonymous", "role": "guest", "team_id": "none", "pii_access": False})

    @staticmethod
    def apply_rls(connector_id: str, data: List[Dict[str, Any]], user_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simulated OPA RLS.
        """
        if user_context["role"] == "guest":
            return []
        
        # Mobile team only sees mobile PRs
        if connector_id == "github":
            return [row for row in data if row["team_id"] == user_context["team_id"]]
        
        # Jira: Mobile team only sees MOBILE project issues
        if connector_id == "jira":
            return [row for row in data if row["project"].lower() == user_context["team_id"]]
            
        return data

    @staticmethod
    def apply_cls(connector_id: str, data: List[Dict[str, Any]], user_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simulated OPA CLS (Masking).
        """
        if not user_context["pii_access"]:
            for row in data:
                if "author_email" in row:
                    row["author_email"] = SecurityEnforcer._mask(row["author_email"])
        
        # Role-based column blocking
        if user_context["role"] == "qa" and connector_id == "github":
            for row in data:
                # QA can see metadata but not "code-level" status/branches in gh if we choose to block it.
                # Let's block 'author' for QA in GitHub for this demonstration.
                row["author"] = "[HIDDEN]"
                
        return data

    @staticmethod
    def _mask(val: str) -> str:
        return hashlib.sha256(val.encode()).hexdigest()[:8] + "****@ema.co"

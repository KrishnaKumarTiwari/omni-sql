"""
Security Tests: RLS and CLS Verification

Tests verify that Row-Level Security (RLS) and Column-Level Security (CLS)
are correctly enforced according to user context and policies.
"""

import pytest
from prototype.utils.security import SecurityEnforcer


class TestAuthentication:
    """Test user authentication and context extraction"""
    
    def test_valid_developer_token(self):
        """Developer token should return correct user context"""
        context = SecurityEnforcer.authenticate("token_dev")
        assert context["user_id"] == "u1"
        assert context["role"] == "developer"
        assert context["team_id"] == "mobile"
        assert context["pii_access"] is True
    
    def test_valid_qa_token(self):
        """QA token should return correct user context"""
        context = SecurityEnforcer.authenticate("token_qa")
        assert context["user_id"] == "u2"
        assert context["role"] == "qa"
        assert context["team_id"] == "mobile"
        assert context["pii_access"] is False
    
    def test_invalid_token(self):
        """Invalid token should return guest context"""
        context = SecurityEnforcer.authenticate("invalid_token")
        assert context["role"] == "guest"
        assert context["pii_access"] is False


class TestRowLevelSecurity:
    """Test RLS filtering based on team and role"""
    
    def test_github_rls_mobile_team(self):
        """Mobile team should only see mobile PRs"""
        user_context = {"role": "developer", "team_id": "mobile"}
        data = [
            {"pr_id": 1, "team_id": "mobile"},
            {"pr_id": 2, "team_id": "web"},
            {"pr_id": 3, "team_id": "mobile"}
        ]
        
        filtered = SecurityEnforcer.apply_rls("github", data, user_context)
        assert len(filtered) == 2
        assert all(row["team_id"] == "mobile" for row in filtered)
    
    def test_github_rls_web_team(self):
        """Web team should only see web PRs"""
        user_context = {"role": "developer", "team_id": "web"}
        data = [
            {"pr_id": 1, "team_id": "mobile"},
            {"pr_id": 2, "team_id": "web"},
            {"pr_id": 3, "team_id": "mobile"}
        ]
        
        filtered = SecurityEnforcer.apply_rls("github", data, user_context)
        assert len(filtered) == 1
        assert filtered[0]["team_id"] == "web"
    
    def test_jira_rls_mobile_team(self):
        """Mobile team should only see MOBILE project issues"""
        user_context = {"role": "developer", "team_id": "mobile"}
        data = [
            {"issue_key": "MOB-1", "project": "MOBILE"},
            {"issue_key": "WEB-1", "project": "WEB"},
            {"issue_key": "MOB-2", "project": "MOBILE"}
        ]
        
        filtered = SecurityEnforcer.apply_rls("jira", data, user_context)
        assert len(filtered) == 2
        assert all(row["project"] == "MOBILE" for row in filtered)
    
    def test_rls_guest_user(self):
        """Guest users should see no data"""
        user_context = {"role": "guest", "team_id": "none"}
        data = [
            {"pr_id": 1, "team_id": "mobile"},
            {"pr_id": 2, "team_id": "web"}
        ]
        
        filtered = SecurityEnforcer.apply_rls("github", data, user_context)
        assert len(filtered) == 0


class TestColumnLevelSecurity:
    """Test CLS masking and column blocking"""
    
    def test_cls_pii_masking_for_qa(self):
        """QA users without PII access should see masked emails"""
        user_context = {"role": "qa", "team_id": "mobile", "pii_access": False}
        data = [
            {"pr_id": 1, "author": "dev1", "author_email": "dev1@company.com"}
        ]
        
        masked = SecurityEnforcer.apply_cls("github", data, user_context)
        assert masked[0]["author_email"] != "dev1@company.com"
        assert "****@ema.co" in masked[0]["author_email"]
    
    def test_cls_no_masking_for_developer(self):
        """Developers with PII access should see real emails"""
        user_context = {"role": "developer", "team_id": "mobile", "pii_access": True}
        data = [
            {"pr_id": 1, "author": "dev1", "author_email": "dev1@company.com"}
        ]
        
        masked = SecurityEnforcer.apply_cls("github", data, user_context)
        assert masked[0]["author_email"] == "dev1@company.com"
    
    def test_cls_author_blocking_for_qa(self):
        """QA users should have author field hidden in GitHub"""
        user_context = {"role": "qa", "team_id": "mobile", "pii_access": False}
        data = [
            {"pr_id": 1, "author": "dev1", "author_email": "dev1@company.com"}
        ]
        
        masked = SecurityEnforcer.apply_cls("github", data, user_context)
        assert masked[0]["author"] == "[HIDDEN]"
    
    def test_cls_no_blocking_for_developer(self):
        """Developers should see author field in GitHub"""
        user_context = {"role": "developer", "team_id": "mobile", "pii_access": True}
        data = [
            {"pr_id": 1, "author": "dev1"}
        ]
        
        masked = SecurityEnforcer.apply_cls("github", data, user_context)
        assert masked[0]["author"] == "dev1"
    
    def test_cls_masking_consistency(self):
        """Same email should produce same hash"""
        user_context = {"role": "qa", "team_id": "mobile", "pii_access": False}
        data = [
            {"pr_id": 1, "author_email": "dev1@company.com"},
            {"pr_id": 2, "author_email": "dev1@company.com"}
        ]
        
        masked = SecurityEnforcer.apply_cls("github", data, user_context)
        assert masked[0]["author_email"] == masked[1]["author_email"]


class TestSecurityIntegration:
    """Test combined RLS + CLS enforcement"""
    
    def test_rls_then_cls_pipeline(self):
        """RLS should filter first, then CLS should mask"""
        user_context = {"role": "qa", "team_id": "mobile", "pii_access": False}
        data = [
            {"pr_id": 1, "team_id": "mobile", "author": "dev1", "author_email": "dev1@company.com"},
            {"pr_id": 2, "team_id": "web", "author": "dev2", "author_email": "dev2@company.com"},
            {"pr_id": 3, "team_id": "mobile", "author": "dev3", "author_email": "dev3@company.com"}
        ]
        
        # Apply RLS first
        filtered = SecurityEnforcer.apply_rls("github", data, user_context)
        assert len(filtered) == 2  # Only mobile team
        
        # Apply CLS second
        masked = SecurityEnforcer.apply_cls("github", filtered, user_context)
        assert len(masked) == 2
        assert all(row["author"] == "[HIDDEN]" for row in masked)
        assert all("****@ema.co" in row["author_email"] for row in masked)

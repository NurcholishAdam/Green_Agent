"""
Network Policy Tests
Green Agent v5.0.0 (Enhanced)

Adds tests for advanced enhancement integration:
- LIMIT Graph
- MODP (Multi-Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi-Teacher On-Policy Distillation with MoE
- Bio-inspired Optimisation
- FlexGen execution backend
"""

import pytest

class TestNetworkPolicy:
    """Test Network Policy configuration"""
    
    def test_network_policy_created(self, k8s_client, test_namespace):
        """Verify NetworkPolicy is created"""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        
        assert policy is not None
        assert 'Ingress' in policy.spec.policy_types
        assert 'Egress' in policy.spec.policy_types
    
    def test_ingress_rules_configured(self, k8s_client, test_namespace):
        """Verify ingress rules are configured"""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        
        assert len(policy.spec.ingress) >= 2
        
        # Should allow dashboard access
        dashboard_ports = [
            p for rule in policy.spec.ingress
            for p in (rule.ports or [])
            if p.port == 8000
        ]
        assert len(dashboard_ports) >= 1
    
    def test_egress_rules_configured(self, k8s_client, test_namespace):
        """Verify egress rules are configured"""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        
        assert len(policy.spec.egress) >= 2
        
        # Should allow DNS
        dns_ports = [
            p for rule in policy.spec.egress
            for p in (rule.ports or [])
            if p.port == 53
        ]
        assert len(dns_ports) >= 1
    
    def test_pod_selector_matches(self, k8s_client, test_namespace):
        """Verify pod selector matches Green Agent pods"""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        
        assert policy.spec.pod_selector.match_labels['app'] == 'green-agent'
    
    def test_namespace_isolation(self, k8s_client, test_namespace):
        """Verify namespace isolation is configured"""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        
        # Should have rules for namespace selector
        has_namespace_selector = False
        for rule in policy.spec.ingress:
            for from_rule in (rule.from_ or []):
                if from_rule.namespace_selector:
                    has_namespace_selector = True
                    break
        
        assert has_namespace_selector

    # ------------------------------------------------------------------
    # Enhanced tests for advanced enhancement integration
    # ------------------------------------------------------------------
    def test_enhanced_labels_present(self, k8s_client, test_namespace):
        """Verify NetworkPolicy has enhancement labels and annotations."""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        # Check labels
        assert policy.metadata.labels.get('enhancements') == 'enabled'
        # Check annotations for advanced modules
        annotations = policy.metadata.annotations or {}
        expected_annotations = {
            'green-agent/limit-graph': 'true',
            'green-agent/modp': 'true',
            'green-agent/rlhf': 'true',
            'green-agent/distillation': 'true',
            'green-agent/bio-inspired': 'true',
            'green-agent/moe': 'true',
            'green-agent/flexgen': 'true',
        }
        for key, value in expected_annotations.items():
            assert annotations.get(key) == value, f"Missing annotation {key}"

    def test_enhanced_ingress_ports(self, k8s_client, test_namespace):
        """Verify additional ingress ports for enhanced modules are present."""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        enhanced_ports = {8080, 50051, 7687, 7474}
        found_ports = set()
        for rule in policy.spec.ingress:
            for port in (rule.ports or []):
                found_ports.add(port.port)
        # At least some enhanced ports should be open
        assert enhanced_ports & found_ports, "No enhanced ports found in ingress rules"

    def test_enhanced_egress_ports(self, k8s_client, test_namespace):
        """Verify additional egress ports for enhanced services are present."""
        policy = k8s_client['networking'].read_namespaced_network_policy(
            name="green-agent-network-policy",
            namespace=test_namespace
        )
        enhanced_egress_ports = {7687, 9443, 443}  # graph DB, enhanced service, FlexGen API
        found_ports = set()
        for rule in policy.spec.egress:
            for port in (rule.ports or []):
                found_ports.add(port.port)
        assert enhanced_egress_ports & found_ports, "No enhanced egress ports found"

    def test_default_deny_policy_present(self, k8s_client, test_namespace):
        """Verify default-deny NetworkPolicy exists with enhancement labels."""
        try:
            deny_policy = k8s_client['networking'].read_namespaced_network_policy(
                name="green-agent-default-deny",
                namespace=test_namespace
            )
        except Exception:
            pytest.fail("Default deny network policy not found")
        assert deny_policy.metadata.labels.get('enhancements') == 'enabled'
        assert deny_policy.spec.pod_selector.match_labels['app'] == 'green-agent'

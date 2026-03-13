"""
Network Policy Tests
Green Agent v5.0.0
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

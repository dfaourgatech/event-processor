from event_processor.core.router import ROUTES, route


def test_all_contracted_types_have_routes(registry):
    for event_type in ["transaction.authorized", "monitoring.heartbeat", "user.action"]:
        assert route(event_type) is not None
        assert registry.has_contract(event_type, 1)


def test_route_targets_are_delivery_namespaced():
    for target in ROUTES.values():
        assert target.startswith("delivery.")


def test_unknown_type_routes_nowhere():
    assert route("billing.invoice.paid") is None

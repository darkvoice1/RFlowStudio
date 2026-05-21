from app.main import create_app


def test_create_app_returns_bootstrap_payload() -> None:
    """验证新后端骨架入口已可被测试框架正常导入。"""
    payload = create_app()

    assert payload == {"status": "bootstrap"}

expected_user_password = "new_user:new_password"


def mock_get_database_creds(vault_path: str):
    return expected_user_password

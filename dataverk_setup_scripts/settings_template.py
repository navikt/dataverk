SETTINGS_TEMPLATE = {
    "index_connections": {},

    "file_storage_connections": {
        "local": {}
    },

    "bucket_storage_connections": {
        "AWS_S3": {},
        "google_cloud": {
            "credentials": {}
        }
    },

    "vault": {},

    "jenkins": {}
}

optional_parameters = {
    "nais_namespace": ('nais_namespace',),
    "elastic_endpoint": ('index_connections', 'elastic_local'),
    "aws_endpoint": ('bucket_storage_connections', 'AWS_S3', 'host'),
    "jenkins_endpoint": ('jenkins', 'url'),
    "vault_auth_uri": ('vault', 'auth_uri'),
    "vault_secrets_uri": ('vault', 'secrets_uri'),
    "vault_auth_path": ('vault', 'vks_auth_path'),
    "vault_kv_path": ('vault', 'vks_kv_path'),
    "vault_role": ('vault', 'vks_vault_role'),
    "vault_service_account": ('vault', 'service_account')
}

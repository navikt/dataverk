import pandas as pd
from nada.datasets.nav_private import Enhetsregisteret


class TestOracleConnector:
    
    def test_get_enhetsregisteret(self):
        cnx = Enhetsregisteret()
        enh = cnx.get_enhetsregisteret()
        assert True

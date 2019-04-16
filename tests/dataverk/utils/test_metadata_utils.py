# -*- coding: utf-8 -*-
# Import statements
# =================
import pandas as pd
from unittest import TestCase
from dataverk.utils import metadata_utils

# Common input parameters
# =======================
filename = "myresource"
name = "name"
df_value = "data"
df = pd.DataFrame.from_dict({name: [df_value]})


class MethodsReturnType(TestCase):
    """
    Tests methods' output types
    """
    def test_get_csv_schema(self):
        csv_schema = metadata_utils.get_csv_schema(df, filename)
        print(csv_schema)
        self.assertIsInstance(csv_schema, dict)
        self.assertEqual({'fields': [{'name': name, 'description': '', 'type': "string"}]}, csv_schema["schema"])



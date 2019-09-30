import pandas as pd
from unittest import TestCase
from dataverk.utils import anonymization

df_in = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})


class MethodsReturnValues(TestCase):

    def test_replace(self):
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': ["*", "*", "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'], lower_limit=4)

        self.assertTrue(df_out.equals(expected_df_out))

    def test_name_replace_env_not_set(self):
        df = pd.DataFrame(data={'name_column1': ["John Doe"], 'name_column2': ["Jane Doe"]})
        with self.assertRaises(EnvironmentError):
            anonymization.name_replace(df, ['name_column1', 'name_column2'])


class MethodsEvaluateInputTypes(TestCase):

    def test_eval_column_type(self):
        df = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                'ints': [1, 2, 3, 4, 5],
                                'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df, eval_column='values', additional_columns=['ints'], lower_limit=4)

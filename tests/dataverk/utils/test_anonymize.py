import pandas as pd
from unittest import TestCase
from dataverk.utils import anonymization

df_in = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})

df_vals = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                             'ints': [1, 2, 3, 4, 5],
                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})


class MethodsReturnValues(TestCase):

    def test_replace(self):
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': ["*", "*", "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'], lower_limit=4)

        self.assertTrue(df_out.equals(expected_df_out))

        def test_additional_columns_types_string(self):
            df_out_both = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ["*", "*", "*", 4, 5],
                                             'floats': ["*", "*", "*", 4.0, 5.0]})

            df_both = anonymization.anonymize_replace(df_vals, eval_column='ints', additional_columns='floats')
            self.assertTrue(df_both.equals(df_out_both))


    def test_name_replace_env_not_set(self):
        df = pd.DataFrame(data={'name_column1': ["John Doe"], 'name_column2': ["Jane Doe"]})
        with self.assertRaises(EnvironmentError):
            anonymization.name_replace(df, ['name_column1', 'name_column2'])


class MethodsEvaluateInputTypes(TestCase):

    def test_eval_column_type(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_vals, eval_column='values')

    def test_additional_columns_types_not_list(self):
        non_valid_additional_list_types = [pd.DataFrame(), {'one': 1}, 2, 2.5]

        for datatype in non_valid_additional_list_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_vals, eval_column='ints', additional_columns=datatype)

    def test_lower_limit(self):
        non_valid_limit_types = [pd.DataFrame(), {'one': 1}, 'word']

        for datatype in non_valid_limit_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_vals, eval_column='ints', lower_limit=datatype)

    def test_column_names(self):
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_vals, eval_column='ints', additional_columns='object')

        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_vals, eval_column='objects', additional_columns='object')

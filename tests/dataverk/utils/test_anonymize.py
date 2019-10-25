import pandas as pd
from unittest import TestCase
from dataverk.utils import anonymization

df_in = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})

df_values = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                               'ints': [1, 2, 3, 4, 5],
                               'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})


class MethodsReturnValues(TestCase):

    def test_replace(self):
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': ["*", "*", "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'], lower_limit=4)

        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_w_defaults(self):
        df_in_anonymizable_cols = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})

        df_out_additional_columns = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1',
                                                                    additional_columns=None)
        self.assertTrue(df_out_additional_columns.equals(expected_df_out))

        df_out_lower_limit = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1',
                                                             lower_limit=4)
        self.assertTrue(df_out_lower_limit.equals(expected_df_out))

        df_out_replace_by = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1',
                                                            replace_by="*")
        self.assertTrue(df_out_replace_by.equals(expected_df_out))

        df_out_all_defaults = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1')
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

    def test_no_change_in_df_in(self):
        df_in_ = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', lower_limit=4)
        self.assertTrue(df_in.equals(df_in_))

    def test_additional_columns_types_string(self):
        df_out_both = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                         'ints': ["*", "*", "*", 4, 5],
                                         'floats': ["*", "*", "*", 4.0, 5.0]})

        df_both = anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats')
        self.assertTrue(df_both.equals(df_out_both))

    def test_replace_by_single_value(self):
        for r_val in [None, 1.5, 5]:
            expected_df_out = pd.DataFrame(data={'col1': [r_val, r_val, r_val, 4, 5, 6],
                                                 'col2': [r_val, r_val, r_val, 67, 765, 1111]})
            df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                     lower_limit=4, replace_by=r_val)

            self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_by_dict(self):
        expected_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6],
                                             'col2': [0, 0, 0, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                     lower_limit=4, replace_by={'col2': 0, 'col1': None})

        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_by_dict_wrong_order(self):
        expected_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6],
                                             'col2': [0, 0, 0, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                     lower_limit=4, replace_by={'col1': None, 'col2': 0})

        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_by_list(self):
        expected_df_out = pd.DataFrame(data={'col1': [0, 0, 0, 4, 5, 6], 'col2': [None, None, None, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                 lower_limit=4, replace_by=[None, 0])

        self.assertTrue(df_out.equals(expected_df_out))

    def test_name_replace_env_not_set(self):
        df = pd.DataFrame(data={'name_column1': ["John Doe"], 'name_column2': ["Jane Doe"]})
        with self.assertRaises(EnvironmentError):
            anonymization.name_replace(df, ['name_column1', 'name_column2'])


class MethodsEvaluateInputTypes(TestCase):

    def test_additional_columns_types_not_list(self):
        non_valid_additional_list_types = [pd.DataFrame(), {'one': 1}, 2, 2.5]

        for data_type in non_valid_additional_list_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns=data_type)

    def test_eval_column_value_type(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='values')

    def test_lower_limit(self):
        non_valid_limit_types = [pd.DataFrame(), {'one': 1}, 'word']

        for data_type in non_valid_limit_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', lower_limit=data_type)

    def test_lower_limit_float_and_int(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ["*", "*", "*", "*", 5],
                                             'floats': ["*", "*", "*", "*", 5.0]})
        df_out_int = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                     additional_columns='floats', lower_limit=5)
        self.assertTrue(df_out_int.equals(expected_df_out))

        df_out_float = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                       additional_columns='floats', lower_limit=5.0)
        self.assertTrue(df_out_float.equals(expected_df_out))

    def test_column_names(self):
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='object')

        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='objects', additional_columns='object')

    def test_column_name_types(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns=3)
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column=4, additional_columns='ints')

    def not_to_replace_by(self):
        not_to_replace_by = [{'1', '3'}, (1, 3), [1, [2, 3]]]
        for r in not_to_replace_by:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by=r)

    def invalid_number_in_replace_by_list_or_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by=[None, None, "*"])

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by=["*"])

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by={'ints': None, 'values': None, 'floats': "*"})

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by={'ints': "*"})

    def invalid_key_names_in_replace_by_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by={'values': "*", 'flots': 0})

    def invalid_key_name_types_in_replace_by_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats', replace_by={'0': "*", 'flots': 0})

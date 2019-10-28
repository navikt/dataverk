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

    def test_replace_label_int(self):
        expected_df_out = pd.DataFrame(data={'col1': [1, 2, "*", 4, 5, 6], 'col2': [33, 44, "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                          label=3)
        df_out_in_list = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                                  label=[3])
        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_in_list.equals(expected_df_out))

    def test_replace_label_list_ints(self):
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': ["*", "*", "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                          label=[1, 2, 3])
        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_label_float(self):
        df_in_ = pd.DataFrame(data={'col1': [1.1, 2.1, "*", 4.1, 5.1, 6.1], 'col2': [33, 44, "*", 67, 765, 1111]})
        expected_df_out = pd.DataFrame(data={'col1': [1.1, 2.1, "*", 4.1, 5.1, 6.1], 'col2': [33, 44, "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace_by_label(df_in_, eval_column='col1', additional_columns=['col2'],
                                                          label=3.1)
        df_out_in_list = anonymization.anonymize_replace_by_label(df_in_, eval_column='col1', additional_columns=['col2'],
                                                                  label=[3.1])
        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_in_list.equals(expected_df_out))

        expected_df_out_int_with_float = pd.DataFrame(data={'col1': [1, 2, "*", 4, 5, 6], 'col2': [33, 44, "*", 67, 765, 1111]})
        df_out = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                          label=3.0)
        self.assertTrue(df_out.equals(expected_df_out_int_with_float))

    def test_replace_label_list_floats(self):
        df_in_ = pd.DataFrame(data={'col1': [1.1, 2.1, 3.1, 4.1, 5.1, 6.1], 'col2': [33, 44, 55, 67, 96, 1111]})
        expected_df_out = pd.DataFrame(data={'col1': [1.1, 2.1, "*", 4.1, '*', 6.1], 'col2': [33, 44, "*", 67, '*', 1111]})

        df_out = anonymization.anonymize_replace_by_label(df_in_, eval_column='col1', additional_columns=['col2'],
                                                          label=[3.1, 5.1])
        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_label_string(self):
        expected_df_out = pd.DataFrame(data={'values': ['*', 'two', 'three', 'four', 'five'],
                                             'ints': ['*', 2, 3, 4, 5],
                                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out = anonymization.anonymize_replace_by_label(df_values, eval_column='values', additional_columns=['ints'],
                                                          label='one')
        df_out_in_list = anonymization.anonymize_replace_by_label(df_values, eval_column='values', additional_columns=['ints'],
                                                                  label=['one'])
        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_in_list.equals(expected_df_out))

    def test_replace_label_list_strings(self):
        expected_df_out = pd.DataFrame(data={'values': ['*', 'two', '*', 'four', '*'],
                                             'ints': ['*', 2, '*', 4, '*'],
                                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out = anonymization.anonymize_replace_by_label(df_values, eval_column='values', additional_columns=['ints'],
                                                          label=['one', 'three', 'five'])
        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_label_NoneType(self):
        df_w_None = pd.DataFrame({'floats': [None, 1.0], 'ints': [1, 1]})
        expected_df_out = pd.DataFrame({'floats': [0.0, 1.0], 'ints': [1, 1]})
        df_out_ = anonymization.anonymize_replace_by_label(df_w_None, eval_column='floats', label=None, replace_by=0)
        df_out_in_list = anonymization.anonymize_replace_by_label(df_w_None, eval_column='floats', label=[None], replace_by=0)
        df_out_in_list_float = anonymization.anonymize_replace_by_label(df_w_None, eval_column='floats', label=[None],
                                                                        replace_by=0.0)

        self.assertTrue(df_out_.equals(expected_df_out))
        self.assertTrue(df_out_in_list.equals(expected_df_out))
        self.assertTrue(df_out_in_list_float.equals(expected_df_out))

        expected_df_out = pd.DataFrame({'floats': ['None', 1.0], 'ints': [1, 1]})
        df_out_ = anonymization.anonymize_replace_by_label(df_w_None, eval_column='floats', label=[None],
                                                           replace_by='None')

        self.assertTrue(df_out_.equals(expected_df_out))

    def test_replace_by_different_label_types(self):
        df_in = pd.DataFrame(data={'labels': ['one', 1, 1.0, 33],
                                   'ints': [1, 2, 3, 4]})
        df_expected_out = pd.DataFrame(data={'labels': ['*', '*', '*', 33],
                                             'ints': ['*', '*', '*', 4]})

        df_out = anonymization.anonymize_replace_by_label(df_in, eval_column='labels', additional_columns='ints',
                                                          label=['one', 1], anonymize_eval=True)
        self.assertTrue(df_out.equals(df_expected_out))

    def test_replace_label_but_not_anonymize_eval_col(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ['*', 2, '*', 4, '*'],
                                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out = anonymization.anonymize_replace_by_label(df_values, eval_column='values', additional_columns=['ints'],
                                                          label=['one', 'three', 'five'], anonymize_eval=False)
        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_w_defaults(self):
        df_in_anonymizable_cols = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})

        #replace by values under lower limit
        df_out_all_defaults = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1')
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

        #replace by labels
        df_in_anonymizable_cols['col3'] = ['one', 'two', 'three', 'four', 'five', 'six']
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8],
                                             'col3': ['one', 'two', 'three', 'four', 'five', 'six']})

        df_out_all_defaults = anonymization.anonymize_replace_by_label(df_in_anonymizable_cols, eval_column='col1',
                                                                       label=[1, 2, 3])
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

    def test_no_change_in_df_in(self):
        df_in_ = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})

        #replace by values under lower limit
        anonymization.anonymize_replace(df_in, eval_column='col1', lower_limit=4)
        self.assertTrue(df_in.equals(df_in_))

        #replace by labels
        anonymization.anonymize_replace_by_label(df_in, eval_column='col1', label=[1, 3])
        self.assertTrue(df_in.equals(df_in_))

    def test_additional_columns_types_string(self):
        df_out_both = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                         'ints': ["*", "*", "*", 4, 5],
                                         'floats': ["*", "*", "*", 4.0, 5.0]})

        #replace values under lower limit
        df_both = anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats')
        self.assertTrue(df_both.equals(df_out_both))

        #replace by labels
        df_both = anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                           label=[1, 2, 3])
        self.assertTrue(df_both.equals(df_out_both))

    def test_replace_by_single_value(self):
        for r_val in [None, 1.5, 5, 'n/a']:
            expected_df_out = pd.DataFrame(data={'col1': [r_val, r_val, r_val, 4, 5, 6],
                                                 'col2': [r_val, r_val, r_val, 67, 765, 1111]})
            df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                     lower_limit=4, replace_by=r_val)
            df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                               label=[1, 2, 3], replace_by=r_val)

            self.assertTrue(df_out.equals(expected_df_out))
            self.assertTrue(df_out_.equals(expected_df_out))

    def test_replace_by_list(self):
        expected_df_out = pd.DataFrame(data={'col1': [0, 0, 0, 4, 5, 6], 'col2': [None, None, None, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                 lower_limit=4, replace_by=[None, 0])
        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           label=[1, 2, 3], replace_by=[None, 0])
        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_.equals(expected_df_out))

    def test_replace_by_dict(self):
        expected_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6],
                                             'col2': [0, 0, 0, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                 lower_limit=4, replace_by={'col2': 0, 'col1': None})
        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           label=[1, 2, 3], replace_by={'col2': 0, 'col1': None})
        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_.equals(expected_df_out))

    def test_replace_by_dict_wrong_order(self):
        expected_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6],
                                             'col2': [0, 0, 0, 67, 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', additional_columns=['col2'],
                                                 lower_limit=4, replace_by={'col1': None, 'col2': 0})
        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           label=[1, 2, 3], replace_by={'col1': None, 'col2': 0})

        self.assertTrue(df_out.equals(expected_df_out))
        self.assertTrue(df_out_.equals(expected_df_out))

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
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns=data_type,
                                                         label=[1, 2, 3])

    def test_eval_column_value_type(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='values')

    def test_valid_anonymization_by_label(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='values', label=['one', 'two', 'three'],
                                                     anonymize_eval=False)

    def test_lower_limit(self):
        non_valid_limit_types = [pd.DataFrame(), {'one': 1}, 'word', {'1', '3'}]

        for data_type in non_valid_limit_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', lower_limit=data_type)

    def test_label_type(self):
        non_valid_label_types = [{'one': 1}, {'1', '3'}, [1, 2]]

        for data_type in non_valid_label_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', label=data_type)

    def test_lower_limit_float_and_int(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ["*", "*", "*", "*", 5],
                                             'floats': ["*", "*", "*", "*", 5.0]})
        df_out_int = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                     additional_columns='floats', lower_limit=5)

        df_out_float = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                       additional_columns='floats', lower_limit=5.0)
        self.assertTrue(df_out_int.equals(expected_df_out))
        self.assertTrue(df_out_float.equals(expected_df_out))

    def test_column_names(self):
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='object')
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='object', label=2)

        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='objects', additional_columns='object')
            with self.assertRaises(ValueError):
                anonymization.anonymize_replace_by_label(df_values, eval_column='objects', additional_columns='object',
                                                         label=2)

    def test_column_name_types(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns=3)
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns=3.0)

        with self.assertRaises(TypeError):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns=3, label=2)
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns=3.0, label=2)

    def test_not_replace_by(self):
        not_to_replace_by = [{'1', '3'}, (1, 3), [1, [2, 3]]]
        for r in not_to_replace_by:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                                replace_by=r)
                with self.assertRaises(TypeError):
                    anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                             label=2, replace_by=r)

    def test_invalid_number_in_replace_by_list_or_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by=[None, None, "*"])
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by=[None, None, "*"])

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by=["*"])
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by=["*"])

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by={'ints': None, 'values': None, 'floats': "*"})
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by={'ints': None, 'values': None, 'floats': "*"})

        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by={'ints': "*"})
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by={'ints': "*"})

    def test_invalid_key_names_in_replace_by_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by={'values': "*", 'flots': 0})
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by={'values': "*", 'flots': 0})

    def test_invalid_key_name_types_in_replace_by_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', additional_columns='floats',
                                            replace_by={'0': "*", 'floats': 0})
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                     label=2, replace_by={'0': "*", 'floats': 0})


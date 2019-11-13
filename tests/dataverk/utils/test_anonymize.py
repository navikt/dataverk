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

    def test_replace_by_single_label(self):
        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace_by_label(df_values, eval_column='values', labels='two')
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_int = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, "*", 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_int = anonymization.anonymize_replace_by_label(df_values, eval_column='ints', labels=2)
        self.assertTrue(df_out_int.equals(exp_df_out_int))

        exp_df_out_float = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                              'ints': [1, 2, 3, 4, 5],
                                              'floats': [1.0, "*", 3.0, 4.0, 5.0]})
        df_out_float = anonymization.anonymize_replace_by_label(df_values, eval_column='floats', labels=2.0)
        self.assertTrue(df_out_float.equals(exp_df_out_float))

    def test_replace_label_NoneType(self):
        df_none = pd.DataFrame({'floats': [None, 1.0], 'ints': [1, 1]})
        expected_df_out = pd.DataFrame({'floats': [0.0, 1.0], 'ints': [1, 1]})
        df_out_none = anonymization.anonymize_replace_by_label(df_none, eval_column='floats', labels=None, replace_by=0)
        self.assertTrue(df_out_none.equals(expected_df_out))

    def test_replace_by_label_list(self):
        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace_by_label(df_values, eval_column='values', labels=['two'])
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', '*', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace_by_label(df_values, eval_column='values', labels=['two', 'four'])
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_num = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, "*", 3, "*", 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_num = anonymization.anonymize_replace_by_label(df_values, eval_column='ints', labels=[2, 4])
        self.assertTrue(df_out_num.equals(exp_df_out_num))

        exp_df_out_num = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, "*", 3.0, "*", 5.0]})
        df_out_num = anonymization.anonymize_replace_by_label(df_values, eval_column='floats', labels=[2, 4])
        self.assertTrue(df_out_num.equals(exp_df_out_num))

        df_float = pd.DataFrame({'floats': [1.0, 2.0, None, 22.0]})
        exp_df_out_float = pd.DataFrame(data={'floats': [1.0, "*", "*", 22.0]})
        df_out_float = anonymization.anonymize_replace_by_label(df_float, eval_column='floats', labels=[2.0, None])
        self.assertTrue(df_out_float.equals(exp_df_out_float))

        df_obj = pd.DataFrame({'labels': ['one', 1, 1.0, 33], 'ints': [1, 2, 3, 4]})
        df_expected_out = pd.DataFrame({'labels': ['*', '*', '*', 33], 'ints': ['*', '*', '*', 4]})
        df_out = anonymization.anonymize_replace_by_label(df_obj, eval_column='labels', additional_columns=['ints'],
                                                          labels=['one', 1], anonymize_eval=True)
        self.assertTrue(df_out.equals(df_expected_out))

    def test_replace_w_defaults(self):
        df_in_anonymizable_cols = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})

        df_out_all_defaults = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1')
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})
        df_out_all_defaults = anonymization.anonymize_replace_by_label(df_in_anonymizable_cols, eval_column='col1',
                                                                       labels=[1, 2, 3])
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

    def test_not_anonymize_eval_col(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ['*', 2, '*', 4, '*'],
                                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out = anonymization.anonymize_replace_by_label(df_values, eval_column='values', ['ints'],
                                                          labels=['one', 'three', 'five'], anonymize_eval=False)
        self.assertTrue(df_out.equals(expected_df_out))

    def test_no_change_in_df_in(self):
        df_in_ = df_in.copy()

        anonymization.anonymize_replace(df_in, eval_column='col1')
        self.assertTrue(df_in.equals(df_in_))

        anonymization.anonymize_replace_by_label(df_in, eval_column='col1', labels=[1, 3])
        self.assertTrue(df_in.equals(df_in_))

    def test_additional_column_not_in_list(self):
        exp_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                        'ints': ["*", "*", "*", 4, 5],
                                        'floats': ["*", "*", "*", 4.0, 5.0]})

        df_out = anonymization.anonymize_replace(df_values, eval_column='ints', 'floats')
        self.assertTrue(df_out.equals(exp_df_out))

        df_out = anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
                                                          labels=[1, 2, 3])
        self.assertTrue(df_out.equals(exp_df_out))

    def test_num_col_names(self):
        df_ = pd.DataFrame(data={0: [1, 2, 4],
                                 1: [2, 3, 5]})
        exp_df_out_ = pd.DataFrame(data={0: [1, 2, 4],
                                         1: ['*', '*', 5]})
        df_out_ = anonymization.anonymize_replace(df_, eval_column=1)
        self.assertTrue(df_out_.equals(exp_df_out_))

        df_out_label = anonymization.anonymize_replace_by_label(df_, eval_column=0, additional_columns=1,
                                                                labels=[1, 2, 3], anonymize_eval=False)
        self.assertTrue(df_out_label.equals(exp_df_out_))

    def test_lower_limit_float_and_int(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ["*", "*", "*", "*", 5],
                                             'floats': ["*", "*", "*", "*", 5.0]})
        df_out_int = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                     'floats', lower_limit=5)
        self.assertTrue(df_out_int.equals(expected_df_out))

        df_out_float = anonymization.anonymize_replace(df_values, eval_column='ints',
                                                       'floats', lower_limit=5.0)
        self.assertTrue(df_out_float.equals(expected_df_out))

    def test_replace_by_single_value(self):
        for r_val in [None, 1.5, 5, 'n/a']:
            expected_df_out = pd.DataFrame(data={'col1': [r_val, r_val, r_val, 4, 5, 6],
                                                 'col2': [r_val, r_val, r_val, 67, 765, 1111]})
            df_out = anonymization.anonymize_replace(df_in, eval_column='col1', ['col2'],
                                                     lower_limit=4, replace_by=r_val)
            self.assertTrue(df_out.equals(expected_df_out))

            df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                               labels=[1, 2, 3], replace_by=r_val)
            self.assertTrue(df_out_.equals(expected_df_out))

    def test_replace_by_list(self):
        exp_df_out = pd.DataFrame(data={'col1': [0, 0, 0, 4, 5, 6], 'col2': [None, None, None, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', ['col2'],
                                                 lower_limit=4, replace_by=[None, 0])
        self.assertTrue(df_out.equals(exp_df_out))

        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           labels=[1, 2, 3], replace_by=[None, 0])
        self.assertTrue(df_out_.equals(exp_df_out))

    def test_replace_by_dict(self):
        exp_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6], 'col2': [0, 0, 0, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', ['col2'],
                                                 lower_limit=4, replace_by={'col2': 0, 'col1': None})
        self.assertTrue(df_out.equals(exp_df_out))

        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           labels=[1, 2, 3], replace_by={'col2': 0, 'col1': None})
        self.assertTrue(df_out_.equals(exp_df_out))

    def test_replace_by_dict_wrong_order(self):
        exp_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6], 'col2': [0, 0, 0, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', ['col2'],
                                                 lower_limit=4, replace_by={'col1': None, 'col2': 0})
        self.assertTrue(df_out.equals(exp_df_out))

        df_out_ = anonymization.anonymize_replace_by_label(df_in, eval_column='col1', additional_columns=['col2'],
                                                           labels=[1, 2, 3], replace_by={'col1': None, 'col2': 0})
        self.assertTrue(df_out_.equals(exp_df_out))

    def test_name_replace_env_not_set(self):
        df = pd.DataFrame(data={'name_column1': ["John Doe"], 'name_column2': ["Jane Doe"]})
        with self.assertRaises(EnvironmentError):
            anonymization.name_replace(df, ['name_column1', 'name_column2'])


class MethodsEvaluateInputTypes(TestCase):

    def test_anonymize_columns_types_not_list(self):
        non_valid_additional_list_types = [pd.DataFrame(), {'one': 1}]

        for data_type in non_valid_additional_list_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', data_type)
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns=data_type,
                                                         labels=[1, 2, 3])

    def test_eval_column_value_type(self):
        with self.assertRaises(TypeError):
            anonymization.anonymize_replace(df_values, eval_column='values')

    def test_valid_anonymization_by_label(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace_by_label(df_values, eval_column='values',
                                                     label=['one', 'two', 'three'],
                                                     anonymize_eval = False)

    def test_lower_limit(self):
        non_valid_limit_types = ['word', pd.DataFrame(), {'one': 1}, {'1', '3'}]

        for data_type in non_valid_limit_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', lower_limit=data_type)

    def test_label_type(self):
        non_valid_label_types = [{'one': 1}, {'1', '3'}, [1, 2]]

        for data_type in non_valid_label_types:
            with self.assertRaises(TypeError):
                anonymization.anonymize_replace(df_values, eval_column='ints', labels=data_type)

    def test_column_names(self):
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='object')
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='object',
                                                     labels=2)

        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='objects', anonymize_columns='ints')
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace_by_label(df_values, eval_column='objects', additional_columns='ints',
                                                     labels=2)

    def test_invalid_key_names_in_replace_by_dict(self):
        with self.assertRaises(Exception):
            anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='floats')
        #with self.assertRaises(Exception):
        #    anonymization.anonymize_replace_by_label(df_values, eval_column='ints', additional_columns='floats',
        #                                             labels=2, replace_by={'values': "*", 'flots': 0})


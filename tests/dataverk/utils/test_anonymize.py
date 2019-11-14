import numpy as np
import pandas as pd
from unittest import TestCase
from dataverk.utils import anonymization

df_in = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})

df_values = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                               'ints': [1, 2, 3, 4, 5],
                               'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})


class MethodsReturnValues(TestCase):

    def test_replace_w_defaults(self):
        df_in_anonymizable_cols = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", 4, 5, 6], 'col2': [3, 4, 5, 6, 7, 8]})

        df_out_all_defaults = anonymization.anonymize_replace(df_in_anonymizable_cols, eval_column='col1')
        self.assertTrue(df_out_all_defaults.equals(expected_df_out))

    def test_replace_based_on_lower_limit(self):
        expected_df_out = pd.DataFrame(data={'col1': ["*", "*", "*", "*", 5, 6],
                                             'col2': ["*", "*", "*", "*", 765, 1111]})
        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', anonymize_columns=['col2'],
                                                 evaluator=lambda x: x < 5)
        self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_based_on_single_label(self):
        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace(df_values, eval_column='values', evaluator=lambda x: x == 'two')
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_int = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, "*", 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_int = anonymization.anonymize_replace(df_values, eval_column='ints', evaluator=lambda x: x == 2)
        self.assertTrue(df_out_int.equals(exp_df_out_int))

    def test_replace_based_on_label_NoneType(self):
        df_none = pd.DataFrame({'floats': [None, 1.0], 'ints': [1, 1]})
        expected_df_out = pd.DataFrame({'floats': [0.0, 1.0], 'ints': [1, 1]})
        df_out_none = anonymization.anonymize_replace(df_none, eval_column='floats', evaluator=lambda x: np.isnan(x),
                                                      replace_by=0)
        self.assertTrue(df_out_none.equals(expected_df_out))

    def test_replace_based_on_label_list(self):
        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace(df_values, eval_column='values', evaluator=lambda x: x in ['two'])
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_str = pd.DataFrame(data={'values': ['one', '*', 'three', '*', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_str = anonymization.anonymize_replace(df_values, eval_column='values',
                                                     evaluator=lambda x: x in ['two', 'four'])
        self.assertTrue(df_out_str.equals(exp_df_out_str))

        exp_df_out_num = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, "*", 3, "*", 5],
                                            'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out_num = anonymization.anonymize_replace(df_values, eval_column='ints', evaluator=lambda x: x in [2, 4])
        self.assertTrue(df_out_num.equals(exp_df_out_num))

        exp_df_out_num = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                            'ints': [1, 2, 3, 4, 5],
                                            'floats': [1.0, "*", 3.0, "*", 5.0]})
        df_out_num = anonymization.anonymize_replace(df_values, eval_column='floats', evaluator=lambda x: x in [2, 4])
        self.assertTrue(df_out_num.equals(exp_df_out_num))

        df_obj = pd.DataFrame({'labels': ['one', 1, 1.0, 33], 'ints': [1, 2, 3, 4]})
        df_expected_out = pd.DataFrame({'labels': ['*', '*', '*', 33], 'ints': ['*', '*', '*', 4]})
        df_out = anonymization.anonymize_replace(df_obj, eval_column='labels', anonymize_columns=['ints'],
                                                 evaluator=lambda x: x in ['one', 1])
        self.assertTrue(df_out.equals(df_expected_out))

    def test_not_anonymize_eval_col(self):
        expected_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                             'ints': ['*', 2, '*', 4, '*'],
                                             'floats': [1.0, 2.0, 3.0, 4.0, 5.0]})
        df_out = anonymization.anonymize_replace(df_values, eval_column='values', anonymize_columns=['ints'],
                                                 evaluator=lambda x: x in ['one', 'three', 'five'],
                                                 anonymize_eval=False)
        self.assertTrue(df_out.equals(expected_df_out))

        df_out = anonymization.anonymize_replace(df_values, eval_column='values', anonymize_columns=['values', 'ints'],
                                                 evaluator=lambda x: x in ['one', 'three', 'five'],
                                                 anonymize_eval=False)
        self.assertTrue(df_out.equals(expected_df_out))

    def test_no_change_in_df_in(self):
        df_in_ = df_in.copy()

        anonymization.anonymize_replace(df_in, eval_column='col1')
        self.assertTrue(df_in.equals(df_in_))

        anonymization.anonymize_replace(df_in, eval_column='col1', evaluator=lambda x: x in [1, 3])
        self.assertTrue(df_in.equals(df_in_))

    def test_anonymize_column_str_name(self):
        exp_df_out = pd.DataFrame(data={'values': ['one', 'two', 'three', 'four', 'five'],
                                        'ints': ["*", "*", "*", 4, 5],
                                        'floats': ["*", "*", "*", 4.0, 5.0]})

        df_out = anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='floats')
        self.assertTrue(df_out.equals(exp_df_out))

        df_out = anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='floats',
                                                 evaluator=lambda x: x in [1, 2, 3])
        self.assertTrue(df_out.equals(exp_df_out))

    def test_anonymize_column_num_name(self):
        df_ = pd.DataFrame(data={0: [1, 2, 4], 1: [2, 3, 5]})
        exp_df_out_ = pd.DataFrame(data={0: [1, 2, 4], 1: ['*', '*', 5]})
        df_out_ = anonymization.anonymize_replace(df_, eval_column=1)
        self.assertTrue(df_out_.equals(exp_df_out_))

    def test_replace_by_single_value(self):
        for r_val in [None, 1.5, 5, 'n/a']:
            expected_df_out = pd.DataFrame(data={'col1': [r_val, r_val, r_val, 4, 5, 6],
                                                 'col2': [r_val, r_val, r_val, 67, 765, 1111]})
            df_out = anonymization.anonymize_replace(df_in, eval_column='col1', anonymize_columns=['col2'],
                                                     replace_by=r_val)
            self.assertTrue(df_out.equals(expected_df_out))

    def test_replace_by_list(self):
        exp_df_out = pd.DataFrame(data={'col1': [0, 0, 0, 4, 5, 6], 'col2': [None, None, None, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', anonymize_columns=['col2'],
                                                 replace_by=[None, 0])
        self.assertTrue(df_out.equals(exp_df_out))

    def test_replace_by_dict(self):
        exp_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6], 'col2': [0, 0, 0, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', anonymize_columns=['col2'],
                                                 replace_by={'col2': 0, 'col1': None})
        self.assertTrue(df_out.equals(exp_df_out))

    def test_replace_by_dict_wrong_order(self):
        exp_df_out = pd.DataFrame(data={'col1': [None, None, None, 4, 5, 6], 'col2': [0, 0, 0, 67, 765, 1111]})

        df_out = anonymization.anonymize_replace(df_in, eval_column='col1', anonymize_columns=['col2'],
                                                 replace_by={'col1': None, 'col2': 0})
        self.assertTrue(df_out.equals(exp_df_out))

    def test_name_replace_env_not_set(self):
        df = pd.DataFrame(data={'name_column1': ["John Doe"], 'name_column2': ["Jane Doe"]})
        with self.assertRaises(EnvironmentError):
            anonymization.name_replace(df, ['name_column1', 'name_column2'])


class MethodsEvaluateInputTypes(TestCase):

    def test_valid_anonymization_by_label(self):
        with self.assertRaises(ValueError):
            anonymization.anonymize_replace(df_values, eval_column='values',
                                            evaluator=lambda x: x in ['one', 'two', 'three'],
                                            anonymize_eval=False)

    def test_column_names(self):
        with self.assertRaises(AttributeError):
            anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='object')

        with self.assertRaises(AttributeError):
            anonymization.anonymize_replace(df_values, eval_column='objects', anonymize_columns='ints')

    def test_invalid_key_names_in_replace_by_dict(self):
        with self.assertRaises(AttributeError):
            anonymization.anonymize_replace(df_values, eval_column='ints', anonymize_columns='floats',
                                            replace_by={'ins': 0})

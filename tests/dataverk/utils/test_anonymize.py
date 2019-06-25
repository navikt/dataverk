import pandas as pd
import numpy as np
from unittest import TestCase
from dataverk.utils import anonymize

df_in = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})


class Base(TestCase):
    def setUp(self):
        pass


class MethodsReturnValues(Base):

    def test_replace(self):
        expected_df_out = pd.DataFrame(data={'col1': [np.nan, np.nan, np.nan, 4, 5, 6], 'col2': [33, 44, 55, 67, 765, 1111]})
        df_out = anonymize.replace(df_in, columns=['col1'], lower_limit=4)

        self.assertTrue(df_out.equals(expected_df_out))

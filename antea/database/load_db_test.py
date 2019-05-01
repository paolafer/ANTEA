import time
import sqlite3

from os.path import join

import numpy as np

from pytest  import fixture
from pytest  import mark

from . import load_db as DB


def test_sipm_pd(db):
    """Check that we retrieve the correct number of SiPMs."""
    sipms = DB.DataSiPM(db.detector)
    columns = ['SensorID', 'ChannelID', 'Active', 'X', 'Y', 'Z', 'adc_to_pes', 'Sigma', 'PhiNumber', 'ZNumber']
    assert columns == list(sipms)
    assert sipms.shape[0] == db.nsipms


def test_mc_runs_equal_data_runs(db):
    assert (DB.DataSiPM(db.detector, -3550).values == DB.DataSiPM(db.detector, 3550).values).all()


def test_prob_list(db):
    prob_lists = DB.ProbabilityList(db.detector)
    assert len(prob_lists) == 11700

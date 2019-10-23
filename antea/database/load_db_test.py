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


def test_sipm_pd_sim_only(db_sim_only):
    """Check that we retrieve the correct number of SiPMs for the full-body configuration."""
    sipms = DB.DataSiPMsim_only(db_sim_only.detector)
    columns = ['SensorID', 'X', 'Y', 'Z', 'PhiNumber', 'ZNumber']
    assert columns == list(sipms)
    assert sipms.shape[0] == db_sim_only.nsipms


def test_mc_runs_equal_data_runs(db):
    assert (DB.DataSiPM(db.detector, -3550).values == DB.DataSiPM(db.detector, 3550).values).all()


def test_prob_list(db_sim_only):
    prob_lists = DB.ProbabilityList(db_sim_only.detector, conf_label='P7R410Z1950mm')
    assert len(prob_lists) == 11700

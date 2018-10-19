
def get_fylke_from_region(region):
    """ Get current name for fylke

    Sør-, og Nord-Trøndelag er slått sammen til Trøndelag 

    """
    if region == 'Sør-Trøndelag (-2017)': return 'Trøndelag'
    if region == 'Nord-Trøndelag (-2017)': return 'Trøndelag'
    if region == 'Finnmark - Finnmárku': return 'Finnmark'
    if region == 'Troms - Romsa': return 'Troms'
    return region




class UnableToDownloadGfsData(Exception):
    def __init__(self, url):
        self.url = url
        Exception.__init__(self, 'Unable to download %s' % url)


class GfsDataUnavailable(Exception):
    def __init__(self, msg, missing_data):
        self.msg = msg
        self.missing_data = missing_data
        Exception.__init__(self, 'Unable to download %s' % msg)

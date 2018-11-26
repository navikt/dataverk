from .datapackage_base import DataPackage


class UpdateDataPackage(DataPackage):

    def __init__(self, settings: dict, envs):
        super().__init__(settings=settings, envs=envs)

    def run(self):
        ''' Entrypoint for dataverk update
        '''

        pass

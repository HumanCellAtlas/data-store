


class Visitation:

    @classmethod
    def InitializeJob(cls):
        raise NotImplementedError


    @classmethod
    def finalize_job(cls):
        raise NotImplementedError


    @classmethod
    def initialize_worker(cls):
        raise NotImplementedError


    @classmethod
    def finalize_worker(cls):
        raise NotImplementedError

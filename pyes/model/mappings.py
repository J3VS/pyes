import logging
from pyes.crud import ESCrudService
from pyes.model.config import get_config
from pyfunk.pyfunk import get_in
from pyes.schema import type_of, checkargs
from pprint import pformat


logger = logging.getLogger(__name__)


class MappingsDiffService:
    @checkargs
    def __init__(self, crud: type_of(ESCrudService)):
        self.crud = crud

    @staticmethod
    def get_properties(mappings):
        return get_in(mappings, ["mappings", "properties"])

    def get_existing_mappings(self):
        for k, v in self.crud.get_mappings().items():
            return self.get_properties(v)

    def get_expected_mappings(self):
        return self.get_properties(get_config(self.crud.index))

    def get_unapplied_additions(self):
        existing_mappings = self.get_existing_mappings()
        unapplied_mappings = {}
        for k, v in self.get_expected_mappings().items():
            if k not in existing_mappings:
                unapplied_mappings[k] = v
        return unapplied_mappings

    def log_unapplied_mappings(self):
        unapplied_mappings = self.get_unapplied_additions()
        logger.info("unapplied mappings:")
        logger.info(pformat(unapplied_mappings))

    def apply_additions(self):
        unapplied_mappings = self.get_unapplied_additions()
        logger.info("applying mappings:")
        logger.info(pformat(unapplied_mappings))
        try:
            self.crud.put_mappings(unapplied_mappings)
            logger.info("mapping applied")
        except:
            logger.logger.exception("mappings failed")

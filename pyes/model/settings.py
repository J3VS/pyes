from pprint import pformat

import logging
from pyes.crud import ESCrudService
from pyes.model.config import get_config
from pyfunk.pyfunk import get_in
from pyes.schema import type_of, checkargs

logger = logging.getLogger(__name__)


class UpdateSettingsService:
    @checkargs
    def __init__(self, crud: type_of(ESCrudService)):
        self.crud = crud

    def get_expected_analysis(self):
        return get_in(get_config(self.crud.index), ["settings", "analysis"])

    def log_expected_analysis(self):
        analysis = self.get_expected_analysis()
        logger.info("expected analysis:")
        logger.info(pformat(analysis))

    def refresh_analysis(self):
        try:
            logger.info("closing index")
            self.crud.close()
            logger.info("updating analysis")
            self.crud.put_settings({
                "analysis": self.get_expected_analysis()
            })
        except:
            logger.logger.exception("error occurred")
        finally:
            logger.info("opening index")
            self.crud.open()

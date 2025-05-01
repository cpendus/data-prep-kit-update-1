# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import json, argparse, pyarrow, yaml, os
from typing import Any
from collections import namedtuple

from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, UnrecoverableException
from dpk_kenlm.info import short_name, description, get_transform_params, get_transform_param_defaults
from dpk_kenlm.kenlm_model import KenlmModel

def text_perplexity(text: str, model: KenlmModel, label: str, default_value: float, logger: Any)-> float:
    return {label: model.get_perplexity(text) if model else default_value}

class KenLMTransform(AbstractTableTransform):
    """
    Implements perplexity with KenLM
    """
    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)
        config = get_transform_param_defaults() | config

        if not config.get("model_path"):
            config["model_path"] = "."
        if not config.get("sentence_piece_model_path"):
            config["sentence_piece_model_path"] = config.get("model_path")

        self.conf = namedtuple("conf", sorted([k for k in config.keys()]))(**config)

        if self.conf.process_languages:
            self.process_languages = set(self.conf.process_languages.split(" "))
        else:
            self.process_languages = None
        self.models = dict()

    def find_file(self, filename: str, path: str):
        self.logger.info(f"looking for {filename} in {path}")
        found = None
        for dirname in path.split(":"):
            fp = os.path.join(dirname, filename)
            if(os.path.isfile(fp)):
                found = fp
                break
        if found:
            self.logger.info(f"found {found}")
        else:
            self.logger.info(f"not found {filename} in {path}")
        return found

    def get_model(self, lang: str):
        if lang in self.models:
            return self.models.get(lang)

        if self.process_languages and lang not in self.process_languages:
            self.logger.info(f"ignoring language: \"{lang}\"")
            self.models[lang] = None
            return None

        kmfn = self.find_file(self.conf.model_file.format(lang=lang), self.conf.model_path)
        spmfn = self.find_file(self.conf.sentence_piece_model_file.format(lang=lang), self.conf.sentence_piece_model_path)

        if not kmfn or not spmfn:
            self.logger.info(f"no model found for language: \"{lang}\"")
            self.models[lang] = None
            return None

        self.logger.info(f"loading model for {lang} from {kmfn} + {spmfn}")

        model = KenlmModel.from_pretrained(kmfn, spmfn)
        self.models[lang] = model
        return model

    def transform(self, table: pyarrow.Table, file_name: str = None) -> tuple[list[pyarrow.Table], dict[str, Any]]:
        TransformUtils.validate_columns(table=table, required=[self.conf.content_column_name, self.conf.lang_column_name])
        if self.conf.output_perplexity_column_name in table.schema.names:
            raise Exception(f"output column \"{self.conf.output_perplexity_column_name}\" already exists")

        self.logger.debug(f"computing perplexity for {len(table)} rows")

        perplexity = pyarrow.Table.from_pylist(list(
            map(lambda x: text_perplexity(x[0], self.get_model(x[1]), self.conf.output_perplexity_column_name, self.conf.default_value, self.logger), 
                zip(table[self.conf.content_column_name].to_pylist(), table[self.conf.lang_column_name].to_pylist()))
        ))

        result = table
        TransformUtils.add_column(table=result, name=self.conf.output_perplexity_column_name, content=perplexity[self.conf.output_perplexity_column_name])
        return [result], {}


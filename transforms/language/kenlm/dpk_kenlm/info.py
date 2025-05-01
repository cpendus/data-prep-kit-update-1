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

import os
from collections import namedtuple

__version__ = "1.0.0"
short_name = "kenlm"
description = "compute perplexity using KenLM models"
ray_invocation = "-m dpk_kenlm.ray.runtime"
invocation = "-m dpk_kenlm.runtime"

Param = namedtuple("Param", "Name Required Type Default Description")
_param_table = [
        Param("content_column_name", False, str, "contents", "Name of the column with the text"),
        Param("default_value", False, float, -1.0, "Perplexity value for when a model is not found"),
        Param("lang_column_name", False, str, "lang", "Name of the column with the language identifier"),
        Param("model_file", False, str, "{lang}.arpa.bin", "Format string to generate the kenlm model file name"),
        Param("model_path", False, str, ".", "Path to search for knlm model files"),
        Param("output_perplexity_column_name", False, str, "kenlm", "Column name to store the perplexity score label"),
        Param("process_languages", False, str, "", "Comma separated list of languages to process. Leave empty to attempt to process all languages."),
        Param("sentence_piece_model_file", False, str, "{lang}.sp.model", "Format string to generate the sentence piece model file name."),
        Param("sentence_piece_model_path", False, str, "", "Path to search for sentence piece model files. Leave empty to search in the kenlm_model_path."),
    ]

def get_transform_params():
    return _param_table

def get_transform_param_defaults():
    return {p.Name: p.Default for p in get_transform_params()}


# KenLM perplexity

Please see the set of
[transform project conventions](../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Contributors

- Cezar Pendus (cpendus@us.ibm.com)

## Summary 

Computes perplexity using KenLM models.

## Required parameters for the transform
| Name  | Default Value | Description |
|------------|----------|--------------|
| **lm_filter_config** | **_text_** | File name with the conditions for the filter, in YAML format. |
| **kenlm_content_column_name** | **lang** | The column name with the text to compute the perplexity for. |
| **kenlm_output_perplexity_column_name** | **kenlm** | The column name with perplexity output. |
| **kenlm_lang_column_name** | **lang** | The column name with language identifier used in the filter conditions. |
| **kenlm_default_value** | **-1.0** | Value to use when a model is not found or processing is disabled. |
| **kenlm_model_file** | **{lang}.arpa.bin** | KenKM model file name pattern. |
| **kenlm_sentence_piece_model_file** | **{lang}.sp.model** | Sentence piece model file pattern. |
| **kenlm_model_path** | **.** | Path to search for KenKM model files. |
| **kenlm_sentence_piece_path** | **kenlm_model_path** | Path to search for sentence piece model files. |
| **pocess_languages** |  | Comma separated list of languages to process. |

## Running the samples
To run the samples, use the following `make` target

* `run-cli-sample` - runs dpk_kenlm.runtime using command line args
or
* `run-ray-cli-sample` - runs dpk_kenlm.ray.runtime using command line args

This target will activate the virtual environment and sets up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.

**Note:** The Ray scalable version of the transform is included, however, running on a single machine and not a cluster, the Ray version does not get a performance benefit because the transform operates one record at a time on an input file.

### Code example

[notebook](./kenlm.ipynb)

## Testing

Following [the testing strategy of data-processing-lib](../../../data-processing-lib/doc/transform-testing.md)

Currently we have:
- [Unit test](test/test_kenlm.py)

## Credits

The work on this transform is continuation of the original work by Juergen Bross (jbross@us.ibm.com).

import copy
import math

import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import BertConfig
from transformers import BertTokenizer

from args import eval_argparser
from config_reader import _read_config, _convert_config
from spert import models
from spert import sampling
from spert import util
from spert.entities import Dataset
from spert.evaluator import Evaluator
from spert.input_reader import JsonInputReader
from utils import prettify
from data_warehouse import integrate 
class SpertClinet:

    def __init__(self, args):
        self.args = args
        self._tokenizer = BertTokenizer.from_pretrained(args.tokenizer_path,
                                                        do_lower_case=args.lowercase,
                                                        cache_dir=args.cache_path)
        self.input_reader = JsonInputReader(types_path=args.types_path,
                                            tokenizer=self._tokenizer,
                                            max_span_size=args.max_span_size)
        self._device = "cpu"#torch.device("cuda" if torch.cuda.is_available() and not args.cpu else "cpu")
        self._load_model()
        print("Model is loaded!")

    def _load_model(self):
        model_class = models.get_model(self.args.model_type)

        config = BertConfig.from_pretrained(self.args.model_path, cache_dir=self.args.cache_path)
        util.check_version(config, model_class, self.args.model_path)

        self.model = model_class.from_pretrained(self.args.model_path,
                                                 config=config,
                                                 # SpERT model parameters
                                                 cls_token=self._tokenizer.convert_tokens_to_ids('[CLS]'),
                                                 relation_types=self.input_reader.relation_type_count - 1,
                                                 entity_types=self.input_reader.entity_type_count,
                                                 max_pairs=self.args.max_pairs,
                                                 prop_drop=self.args.prop_drop,
                                                 size_embedding=self.args.size_embedding,
                                                 freeze_transformer=self.args.freeze_transformer,
                                                 cache_dir=self.args.cache_path)

        self.model.to(self._device)

    def forward(self, tokens):

        self.input_reader.read_for_infer(tokens)
        dataset = self.input_reader.get_dataset("infer")
        dataset.switch_mode(Dataset.EVAL_MODE)
        data_loader = DataLoader(dataset, batch_size=self.args.eval_batch_size, shuffle=False, drop_last=False,
                                 num_workers=self.args.sampling_processes, collate_fn=sampling.collate_fn_padding)

        evaluator = Evaluator(dataset, self.input_reader, self._tokenizer,
                              self.args.rel_filter_threshold, self.args.no_overlapping, None,
                              "predictions_%s_epoch_%s.json", self.args.example_count, 0, dataset.label)

        with torch.no_grad():
            self.model.eval()

            # iterate batches
            total = math.ceil(dataset.document_count / self.args.eval_batch_size)
            for batch in tqdm(data_loader, total=total, desc='Evaluate epoch %s' % 0):
                # move batch to selected device
                batch = util.to_device(batch, self._device)

                # run model (forward pass)
                result = self.model(encodings=batch['encodings'], context_masks=batch['context_masks'],
                                    entity_masks=batch['entity_masks'], entity_sizes=batch['entity_sizes'],
                                    entity_spans=batch['entity_spans'],
                                    entity_sample_masks=batch['entity_sample_masks'],
                                    evaluate=True)
                entity_clf, rel_clf, rels = result
                evaluator.eval_batch(entity_clf, rel_clf, rels, batch)

        return evaluator.get_preds()

    def __call__(self, texts):
        if isinstance(texts, str):
            texts = [texts]
        tokens = [text.split() for text in texts]
        return self.forward(tokens)


def process_config(config_path=None):
    arg_parser = eval_argparser()
    args, _ = arg_parser.parse_known_args()
    if config_path:
        args.__setattr__("config", config_path)
    config = _read_config(args.config)

    _, run_config = config[0]
    args_copy = copy.deepcopy(args)
    config_list = _convert_config(run_config)
    run_args = arg_parser.parse_args(config_list, namespace=args_copy)
    run_args_dict = vars(run_args)

    # set boolean values
    for k, v in run_config.items():
        if v.lower() == 'false':
            run_args_dict[k] = False
    return run_args, run_config


#if __name__ == "__main__":
#    run_args, run_config = process_config()
#    spert_clinet = SpertClinet(run_args)
#    preds = spert_clinet(["Trump kill Mickel Jeckson", "Alice worked at SturBucks", "David lives in Goris city."])
#    _, rel_triples = prettify(preds)
#    integrate(rel_triples)

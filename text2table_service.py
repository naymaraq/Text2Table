from flask import Flask, request, jsonify
from one_shot_infer import SpertClinet, process_config
from utils import prettify
app = Flask(__name__)

config_path = "configs/example_eval.conf"
run_args, run_config = process_config(config_path)
spert_clinet = SpertClinet(run_args)

@app.route("/")
def home():
    text = request.args.get('text')
    result = spert_clinet(text)
    pretty_json, rel_triples = prettify(result)
    return jsonify(pretty_json)


if __name__ == "__main__":
    app.run(host="localhost", port=6969)

import evaluate
from time import ctime, sleep, time


def exp1(top_k=10, positions=None):
    if positions is None:
        positions = [1, 3, 5, 10]
    ds_path = './datasets/orkg/ORKG-QA-DS.csv'
    results = evaluate.evaluate_random_baseline_efficient(ds_path, top_k)
    final_result = f'Random:\nPrecision: {results[0][1]:.4f},\tRecall: {results[0][2]:.4f},\tF1-Score: {results[0][2]:.4f}\n'
    print("Done with Random!!!")
    final_result = f'{final_result}{"=" * 40}\n'
    results = evaluate.evaluate_lucene_baseline_efficient(ds_path, top_k)
    results = [x for x in results if x[0] in positions]
    for result in results:
        final_result = f'{final_result}Lucene:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
        print(f"Done with Lucene@{result[0]}")
    final_result = f'{final_result}{"=" * 40}\n'
    for kind in ['normal', 'aggregation', 'related', 'similar']:
        results = evaluate.evaluate_jarvis_efficient(ds_path, top_k, qtype=kind)
        results = [x for x in results if x[0] in positions]
        for result in results:
            final_result = f'{final_result}Jarvis {kind}:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
            print(f"Done with Jarvis {kind}@{result[0]}")
        final_result = f'{final_result}{"*" * 30}\n'
    results = evaluate.evaluate_jarvis_efficient(ds_path, top_k)
    results = [x for x in results if x[0] in positions]
    for result in results:
        final_result = f'{final_result}Jarvis:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
        print(f"Done with Jarvis overall@{result[0]}")
        final_result = f'{final_result}{"=" * 40}\n'
    with open('benchmark-results-exp1-test.txt', 'w') as out_file:
        out_file.write(final_result)


def exp2(top_k=10, positions=None):
    if positions is None:
        positions = [1, 3, 5, 10]
    ds_path = './datasets/sciqa-mod/SciQA-DS.csv'
    final_result = ''
    # for model in ['bert-large-uncased-whole-word-masking-finetuned-squad',
    #               'bert-large-cased-whole-word-masking-finetuned-squad', 'deepset/bert-base-cased-squad2',
    #               'deepset/bert-large-uncased-whole-word-masking-squad2', 'distilbert-base-uncased-distilled-squad',
    #               'ktrapeznikov/albert-xlarge-v2-squad-v2', 'replydotai/albert-xxlarge-v1-finetuned-squad2']:
    with open('benchmark-results-exp2-lcs-without-split-cpu.txt', 'w+') as out_file:
        starting_time = time()
        out_file.write(f"Starting now: {ctime(starting_time)}\n")
        for model in ['bert-large-cased-whole-word-masking-finetuned-squad']:
            starting_time_model = time()
            out_file.write(f"Starting now for model {model}: {ctime(starting_time_model)}\n")
            final_result = f'{final_result}On model: {model}\n'
            print(f'Starting with model {model}')
            for kind in ['normal', 'aggregation', 'related', 'similar']:
                results = evaluate.evaluate_jarvis_efficient(ds_path, top_k, qtype=kind, model_name=model)
                results = [x for x in results if x[0] in positions]
                for result in results:
                    final_result = f'{final_result}Jarvis {kind}:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
                    print(f"Done with Jarvis {kind}@{result[0]}")
                final_result = f'{final_result}{"*" * 30}\n'
            results = evaluate.evaluate_jarvis_efficient(ds_path, top_k, model_name=model)
            results = [x for x in results if x[0] in positions]
            for result in results:
                final_result = f'{final_result}Jarvis:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
                print(f"Done with Jarvis overall@{result[0]}")
            final_result = f'{final_result}{"=" * 40}\n'
            ending_time_model = time()
            out_file.write(f"Done with model {model} on time {ctime(ending_time_model)}\n")
        ending_time = time()
        out_file.write(final_result)
        out_file.write(f"Done for all models in time {ctime(ending_time)}\n")          


def exp4(top_k=10, positions=None):
    if positions is None:
        positions = [1, 3, 5, 10]
    ds_path = 'datasets/TabMCQ/TabMCQ-DS.csv'
    results = evaluate.evaluate_random_baseline_efficient(ds_path, top_k, ext='tsv')
    final_result = f'Random:\nPrecision: {results[0][1]:.4f},\tRecall: {results[0][2]:.4f},\tF1-Score: {results[0][2]:.4f}\n'
    print("Done with Random!!!")
    final_result = f'{final_result}{"=" * 40}\n'
    results = evaluate.evaluate_lucene_baseline_efficient(ds_path, top_k, ext='tsv')
    results = [x for x in results if x[0] in positions]
    for result in results:
        final_result = f'{final_result}Lucene:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
        print(f"Done with Lucene@{result[0]}")
    final_result = f'{final_result}{"=" * 40}\n'
    results = evaluate.evaluate_jarvis_efficient(ds_path, top_k, ext='tsv')
    results = [x for x in results if x[0] in positions]
    for result in results:
        final_result = f'{final_result}Jarvis:\nk={result[0]}\tPrecision: {result[1]:.4f},\tRecall: {result[2]:.4f},\tF1-Score: {result[3]:.4f}\n'
        print(f"Done with Jarvis overall@{result[0]}")
        final_result = f'{final_result}{"=" * 40}\n'
    with open('benchmark-results-exp4.txt', 'w') as out_file:
        out_file.write(final_result)


if __name__ == '__main__':
    # exp1()
    exp2()
    # exp4()

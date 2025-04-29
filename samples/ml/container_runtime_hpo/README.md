# Container Runtime HPO Documentation 


## Introduction

The Snowflake ML Hyperparameter Optimization (HPO) API is a model-agnostic framework that enables efficient, parallelized hyperparameter tuning of any model using popular tuning algorithms. The HPO workload, initiated from the Notebook, executes inside Snowpark Container Services where it can execute on both CPU and GPU instances. The HPO scales out to as many cores (CPUs or GPUs) as are available on a single node in the SPCS compute pool. It also supports scaling across multiple nodes when needed.



Benefits include:
* **Easy to use**: With a single API, all the complexities of distributing the training across multiple resources are automatically handled by Snowflake
* **Model agnostic**: Users can train with any framework or algorithm, open source or Snowflake ML APIs
* **Flexibility to use many tuning and sampling options:** The API supports Bayesian and random search algorithms, along with various continuous and non-continuous sampling functions.
* **Tightly integrated with the rest of Snowflake**: By passing in Snowflake Datasets or Dataframes into the HPO API users can benefit from efficient Snowflake data ingestion. This also facilitates automatic ML lineage capture.


## Getting Started

Today, this API is only available for use within a Snowflake Notebook configured to use the Container Runtime. Once you [create such a notebook](https://docs.snowflake.com/user-guide/ui-snowsight/notebooks-on-spcs), you can:

* Train a model using any open source package, and use this API to distributed the hyperparameter processing
* Train a model using Snowflake ML distributed training APIs, and scale HPO while also scaling each of the training runs


## Core Concepts


### Dataset Map

**Purpose:**

The dataset_map pairs the train/test datasets with their corresponding Snowflake DataConnector objects, which enables efficient data ingestion from Snowflake.  It’s passed to your training function for seamless data integration. Please refer to [https://docs.snowflake.com/en/developer-guide/snowflake-ml/framework-connectors](https://docs.snowflake.com/en/developer-guide/snowflake-ml/framework-connectors) for various ways of creating Data Connector objects.

**Usage:**
```

dataset_map = {
    "x_train": DataConnector.from_dataframe(session.create_dataframe(X_train)),
    "y_train": DataConnector.from_dataframe(
        session.create_dataframe(y_train.to_frame())
    ),
    "x_test": DataConnector.from_dataframe(session.create_dataframe(X_test)),
    "y_test": DataConnector.from_dataframe(
        session.create_dataframe(y_test.to_frame())
    ),
}
```


### Search Algorithm

**Purpose**: \
The SearchAlgorithm component governs the strategy used to explore the hyperparameter space. It determines how new hyperparameter configurations are selected based on previous trial outcomes.

**Types:**

**Grid Search** 

Grid Search systematically explores a predefined grid of hyperparameter values. Every possible combination is evaluated. 
```
search_space = {
    "n_estimators": [50, 51],
    "max_depth": [4, 5],
    "learning_rate": [0.01, 0.03]
}
```
In this example, each parameter has 2 possible values, so the total number of unique combinations is 2 × 2 × 2 = 8.



**Bayesian Optimization**


Bayesian Optimization leverages probabilistic models to efficiently search the hyperparameter space. By using past evaluations, it predicts which regions are most promising and focuses on those areas. For additional details on how Bayesian Optimization works, please refer to [https://github.com/bayesian-optimization/BayesianOptimization](https://github.com/bayesian-optimization/BayesianOptimization).

**Random Search**

Samples hyperparameters randomly. This approach is simple and effective, especially when dealing with large or mixed (continuous and discrete) search spaces.

**Usage:**
```
from entities import search_algorithm
search_alg = search_algorithm.BayesOpt() 
search_alg = search_algorithm.RandomSearch()
search_alg = search_algorithm.GridSearch()
```


### SearchSpace

**Purpose** \
Search space functions define how hyperparameters are sampled during each trial. They provide a way to describe the range and type of values that hyperparameters can take.

**Supported Functions:**
* uniform(lower, upper):
  * Samples a continuous value uniformly between `lower` and `upper`. Useful for parameters like dropout rates or regularization strengths.
* loguniform(lower, upper):
  * Samples a value in logarithmic space, ideal for parameters that span several orders of magnitude (e.g., learning rates).

* randint(lower, upper):
  * Samples an integer uniformly between `lower` (inclusive) and `upper` (exclusive). Suitable for discrete parameters like the number of layers.

* choice(options):
  * Randomly selects a value from a provided list. Often used for categorical parameters.

**Usage**: \
Define your search space by mapping hyperparameter names to one of these functions: 

```
search_space = {
    "n_estimators": tune.uniform(50, 200),
    "max_depth": tune.uniform(3, 10),
    "learning_rate": tune.uniform(0.01, 0.3),
}
```

### TunerConfig

**Purpose**: \
The **TunerConfig** class is used to configure the tuning process. It specifies which metric to optimize, the optimization mode, and other execution parameters.

**Key Attributes:**

    * Metric: The performance metric (e.g., accuracy, loss) that you want to optimize.
    * Mode: Determines whether the objective is to maximize or minimize the metric (`"max"` or `"min"`).
    * Search Algorithm: Specifies the strategy for exploring the hyperparameter space (e.g., Bayesian Optimization, Random Search, Grid Search).
    * Number of Trials: Sets the total number of hyperparameter configurations to evaluate.
    * Concurrency: Defines how many trials can run concurrently.

**Usage:**

```
from snowflake.ml.modeling import tune
tuner_config = tune.TunerConfig(
    metric="accuracy",
    mode="max",
    search_alg=search_algorithm.BayesOpt(
        utility_kwargs={"kind": "ucb", "kappa": 2.5, "xi": 0.0}
    ),
    num_trials=5,
    max_concurrent_trials=1,
)

```



### TunerContext

**Purpose**: \
The **TunerContext** provides trial-specific context within the training function. It bridges the gap between the HPO framework and the user's training code.

**Responsibilities:**

* Hyperparameter Injection: \
The `get_hyper_params()` method retrieves the specific set of hyperparameters selected for the current trial.
* Dataset Provisioning: \
The `get_dataset_map()` method supplies the training and testing datasets as defined by the user.
* Reporting: \
The `report()` method allows the training function to send back performance metrics (and optionally, the trained model) to the HPO framework. This feedback is used to guide subsequent trials.

**Usage Example in a Training Function**:
```
def train_func():
    tuner_context = get_tuner_context()
    config = tuner_context.get_hyper_params()
    dm = tuner_context.get_dataset_map()
    ...
    tuner_context.report(metrics={"accuracy": accuracy}, model=model)

```




### Tuner

**Purpose**: \
The **Tuner** serves as the entry point for initiating an HPO job. It encapsulates the entire tuning process by tying together the training function, the search space for hyperparameters, and the tuning configuration.

**Responsibilities:**



* **Initiate Trials:** Automatically iterates over different hyperparameter combinations by sampling from the defined search space.
* **Manage Execution:** Distributes the training function across available resources, ensuring that each trial runs with the specified parameters.
* **Aggregate Results:** Collects and summarizes trial outcomes, identifying the best performing configuration.

**Usage:**

Instantiate a `Tuner` object with your training function, search space, and tuning configuration, then call its `run()` method to execute the HPO process.

```
from snowflake.ml.modeling import tune
tuner = tune.Tuner(train_func, search_space, tuner_config)
tuner_results = tuner.run(dataset_map=dataset_map)
```


### TunerResults

**Purpose:**

After all trials are completed, the **TunerResults** object consolidates the outcomes of each trial. It provides structured access to the performance metrics, the best configuration, and the best model.

**Key Attributes:**



* **<code>results</code>:** A Pandas DataFrame containing metrics and configurations for every trial.
* **<code>best_result</code>:** A DataFrame row summarizing the trial with the best performance.
* **<code>best_model</code>:** The model instance associated with the best trial, if applicable.

**Usage:**

```
print(tuner_results.results)
print(tuner_results.best_model)
print(tuner_results.best_result)
```

## Step-by-Step HPO Usage Example
See the [HPO Notebook Example](./hpo_example.ipynb) for a full walkthrough of a HPO run.


## API Reference

### [TBD] Official documentation WIP.  



## Known Limitations



* Bayesian optimization only works with the Uniform sampling function. 
    * Reasoning: Bayesian optimization typically works only with continuous search spaces due to its reliance on Gaussian Processes as surrogate models. Consequently, it is incompatible with discrete parameters sampled from randint or choice functions. To circumvent this limitation, either use tune.uniform and cast the parameter inside the training function, or switch to a sampling algorithm that handles both discrete and continuous spaces, such as RandomSearch.


## Troubleshooting


<table>
  <tr>
   <td>Problem/Error Message
   </td>
   <td>Possible Causes
   </td>
   <td>Resolution
   </td>
  </tr>
  <tr>
   <td>Invalid search space configuration: BayesOpt requires all sampling functions to be of type ‘Uniform’.
   </td>
   <td>As mentioned in the known limitation section BayesOpt doesn’t work with  discrete samples. It works only with “uniform” sampling function.
   </td>
   <td>
<ul>

<li>Use tune.uniform() and then cast the result in your training function.</li>

<li>Switch to RandomSearch algorithm which takes in both discrete and non-discrete samples. </li>
</ul>
   </td>
  </tr>
  <tr>
   <td>Insufficient CPU resources. Required: 16, Available: 8.
<p>
To resolve this, you can try:
<p>
• Reduce num_workers (4).
<p>
• Reduce num_cpu_per_worker (4).
<p>
• Reduce max_concurrent_trials (2).
<p>
• Increase the number of available CPUs by upgrading the compute pool.
<p>
Current configuration: num_workers=4, num_cpu_per_worker=4, max_concurrent_trials=2
   </td>
   <td>This is likely caused by setting <code>max_concurrent_trials</code> too high.
   </td>
   <td>Follow the error message to try one of the options. 
   </td>
  </tr>
  <tr>
   <td>Insufficient GPU resources. Required: 4, Available: 2.
<p>
To resolve this, you can try:
<p>
• Reduce num_workers (4).
<p>
• Reduce num_gpu_per_worker (1).
<p>
• Reduce max_concurrent_trials (2).
<p>
• Increase the number of available GPUs by upgrading the compute pool.
<p>
Current configuration: num_workers=4, num_gpu_per_worker=1, max_concurrent_trials=2
   </td>
   <td>This is likely caused by setting <code>max_concurrent_trials</code> too high.
   </td>
   <td>Follow the error message to try one of the options. 
   </td>
  </tr>
</table>
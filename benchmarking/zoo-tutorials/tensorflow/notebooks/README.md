# Distributted basic classification with Analytics Zoo
## Install Analytics Zoo
To install the Analytics Zoo, please follow [here](https://analytics-zoo.github.io/0.4.0/#PythonUserGuide/install/). We recommend you to install using non-pip method by cloning Analytcis Zoo repo so that you could always use the newest features.

## Dependencies
* TensorFlow 1.10.0
* matplotlib
* sklearn
* Pillow
* pandas


## Setup Analytics Zoo
After installation, please follow [here](https://analytics-zoo.github.io/0.4.0/#PythonUserGuide/run/) to setup the environment.



## Run with jupyter notebook
To run notebook with Analytics Zoo environment, we need to use `dist/bin/jupyter-with-zoo.sh` to start the jupyter notebook kernel.
```shell
chmod +x analytics-zoo/dist/bin/jupyter-with-zoo.sh
analytics-zoo/dist/bin/jupyter-with-zoo.sh
```

## Note
For distributted training, evaluation, and prediction, you should use tensorflow 1.10.0. You could install the specific tensorflow with `pip install tensorflow==1.10.0`.

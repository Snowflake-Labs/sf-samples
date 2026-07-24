# CIFAR-10 Computer Vision Model Training and Evaluation

This repository contains a Jupyter notebook that implements a Convolutional Neural Network (CNN) for image classification using the **CIFAR-10 dataset**. It provides a comprehensive machine learning pipeline, from data loading and preprocessing to model training, evaluation, and prediction.

-----

## 🚀 Getting Started

To use this notebook, you'll need to install the necessary libraries:

```bash
pip install tensorflow scikit-learn
```

These commands will install **TensorFlow**, the deep learning framework used for building and training the CNN, and **Scikit-learn**, which is used for calculating evaluation metrics.

-----

## 📋 Key Components

The notebook is structured into several key sections to guide you through the process:

### 1\. Data Preprocessing

The code loads the CIFAR-10 dataset, which consists of 60,000 32x32 color images across 10 classes. The pixel values of these images are then normalized from the range `[0, 255]` to `[0, 1]` to enhance training performance. The dataset uses sparse categorical labels, which are integers from 0 to 9, instead of one-hot encoding.

### 2\. CNN Architecture

The model is a sequential CNN with the following layers:

  * **Input Layer**: Accepts images of size 32x32 with 3 color channels (RGB).
  * **Convolutional Layers**: Two `Conv2D` layers (32 and 64 filters) with ReLU activation, designed to extract features from the images.
  * **Pooling Layers**: Two `MaxPooling2D` layers that reduce the spatial dimensions of the feature maps, helping to decrease computational complexity and prevent overfitting.
  * **Dense Layers**: Fully connected layers that interpret the features and output a final prediction using a 10-class softmax activation function.

### 3\. Optimizer Comparison

The notebook includes three optimizer options with different performance characteristics:

  * **Adam**: Offers the fastest convergence, typically achieving an accuracy of 60-65% in 20-30 epochs. It is also the least sensitive to the learning rate.
  * **RMSprop**: Has medium convergence speed, reaching 60-65% accuracy in about 30 epochs.
  * **SGD**: Has the slowest convergence, requiring more than 50 epochs to achieve an accuracy of 65-70%. It is very sensitive to the learning rate.

-----

## 📊 Model Evaluation and Prediction

### Training and Saving

The model is compiled with `sparse_categorical_crossentropy` loss and trained with validation on a test set to monitor for overfitting. After training, the model is saved to the `/saved_model/cifar10_model.keras` file.

### Comprehensive Evaluation

After training, the model is evaluated using a variety of metrics provided by Scikit-learn, including:

  * **Accuracy**: Measures the overall correctness of the model's predictions.
  * **Precision, Recall, and F1-Score**: These metrics are calculated with weighted averaging to provide a comprehensive view of performance across all classes.
  * **Confusion Matrix**: Provides a detailed breakdown of the classification results, showing which classes are being confused with others.

### Image Prediction

The notebook also includes a function to load the trained model and make predictions on new, single images. This process requires a crucial preprocessing step: any new image must be resized to 32x32 pixels to match the dimensions the model was trained on. This ensures consistency and compatibility.

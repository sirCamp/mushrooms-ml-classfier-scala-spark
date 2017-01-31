# Mushrooms Machine Learning Classfiers in Scala and Spark

This repo is started as university study for Machine Learning course.
Main goal is comparing different machine learning algorithms during classification by poisoning
of Mushroom dataset.

The dataset is composed by ~8000 items each one with it owns attributes, so before start 
the processes we need some data normalizations.

The used tecnologies, as mentioned, are Scala and Spark, but the algorithms was already tested in Python and Sklearn.



### How to use
To use and modify the project you have just to clone the repo and import the gradle project and
in the end, run and compile the Application.scala file.

###Target
The goal What am set and state What to use and implement four types of algorithms (with different approaches):
Decision tree: with Gini approaches, variance and Entropy
+ Neural networks: Approach MultiPerceptron
+ Bayes: NaiveBayes scam
+ Clustering: Kmeans and Kmeans AngleBisector
Comparison is mediated comparison of "accuracy"

###Features
As regards to features, to make them comparable, they have been transformed using "Feature Extraction" methods and not using One Hot.
Using this approach, Especially As regards Trees, already tested in Python, get performance decidedly superiors in terms of precision of a tree Equal Amplitude and / or elmenti train.

###Hyper Parameters
With regard to the hyper-parameters we sono stati set with All These values, Which I Observed to be among the best:
+ maxDepth (Decision Trees) = 4
+ K (Kmeans) = 2
+ maxIteration (Kmeans) = 20
+ maxIteration (Neural Networks for gradient descent) = 20
By changing These parameters,in some cases, you can Obtain the best performance, all to the detriment in performance.

###Comparison
In order to compare "equal to" the different algorithms, it is necessary that the datasets and other factors are common, such as:

+ Noise (same noise )
+ Training (same random quantity)
+ Test (same random quantity)
+ Features (same features)

To have a real comparison can I Decided to test the algorithms with different Amounts of training so you can get the accuracy Obtained on the basis
the train of the amount Compared to the total dataset.

To do so, the algorithms were run with different Amounts of training for learning:

+ 10% of dataset
+ 20% of dataset
+ 30% of dataset
+ 40% of dataset
+ 50% of dataset
+ 60% of dataset
+ 70% of dataset
+ 80% of dataset
+ 90% of dataset

For every step I recorded the accuracy in the case of clustering the RandIndex That can be Compared all'accuracy, and I printed several graphics, all with
accuracy as ordinates and percentage of train on the x-axis.




#RxAerospike
[![Build Status](https://travis-ci.org/Ganet/rxaerospike.svg?branch=develop)](https://travis-ci.org/Ganet/rxaerospike)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.ganet/rxaerospike/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.ganet/rxaerospike)
[![GitHub license](https://img.shields.io/github/license/kotlintest/kotlintest.svg)]()

RxAerospike is a wrapper for [aerospike-client-java](https://github.com/aerospike/aerospike-client-java) which re-implements the database operations using RxJava2 and Kotlin.

#### Current features

* RxAerospike implements all **non-deprecated** operations from [IAsyncClient](https://github.com/aerospike/aerospike-client-java/blob/master/client/src/com/aerospike/client/async/IAsyncClient.java) and every operation returning a [Task](https://github.com/aerospike/aerospike-client-java/blob/master/client/src/com/aerospike/client/task/Task.java). 
* Synchronous operations have not been implemented yet, but they will be. Also, certain scan operations are implemented in a strange way, which does not send onCompleted() or onFailure() notifications (although they can throw exceptions), and this increases the difficulty of wrapping them.
* All the other operations implemented in AerospikeClient and AsyncClient are still available in this wrapper (synchronous, admin and scan operations from [IAerospikeClient](https://github.com/aerospike/aerospike-client-java/blob/master/client/src/com/aerospike/client/IAerospikeClient.java)), but not implemented using RxJava.

#### Why Kotlin?

* Kotlin makes it very easy to implement [class delegation](https://kotlinlang.org/docs/reference/delegation.html) by eliminating all the boilerplate code.
* Also Kotlin supports default parameters, which generate different methods auto-magically in Java by using the annotation [@JvmOverloads](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.jvm/-jvm-overloads/), reducing boilerplate code even more.

#### Maven central

		<dependency>
			<groupId>com.github.ganet</groupId>
			<artifactId>rxaerospike</artifactId>
			<version>0.1.0</version>
		</dependency>

#### FAQ
**Why not implement the wrapper using Kotlin extension functions?**
In a Kotlin world, that would be the way to go and the ideal implementation. However, this wrapper is intended to be compatible with Java applications as well, so class delegation is a better option.

#### Contact and contributions
I will setup a contribution model soon, please use this mailing list for any questions for now: https://groups.google.com/forum/#!forum/rxaerospike


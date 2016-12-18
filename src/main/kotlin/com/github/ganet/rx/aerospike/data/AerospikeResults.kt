/*
 * Copyright 2016 Jose Ignacio Acin Pozo. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ganet.rx.aerospike.data

import com.aerospike.client.Key

/**
 * Created by JoseIgnacio on 18/12/2016.
 */
data class AerospikeResult<out V> (val key : Key, val result: V)
data class AerospikeArrayResult<out V> (val keys : Array<Key>, val result: V)
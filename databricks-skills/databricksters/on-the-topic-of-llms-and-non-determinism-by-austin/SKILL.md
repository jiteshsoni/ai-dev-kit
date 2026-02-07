---
name: on-the-topic-of-llms-and-non-determinism-by-austin
description: 
---

# On the Topic of LLMs and Non-Determinism: - by Austin

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/on-the-topic-of-llms-and-non-determinism

## Tags
mlops, ai, data-engineering, performance, databricks

## Full Content

# On the Topic of LLMs and Non-Determinism: - by Austin

**Source:** https://www.databricksters.com/p/on-the-topic-of-llms-and-non-determinism

---



On the Topic of LLMs and Non-Determinism: - by Austin

Databricksters

Subscribe

Sign in

AI &amp; ML

On the Topic of LLMs and Non-Determinism:

Practical Limitations in Combating the Myth of Uncertainty in Deep Learning

Austin

Mar 18, 2025

2

1

Share

Have you ever been told that Deep Learning algorithms are inherently non-deterministic? Or that GPUs, PyTorch, TensorFlow, or LLMs are? Until recently, I also believed it to be an unfortunate fact of life that due to both hardware and software limitations I didn't fully understand, there was no way to get deterministic output from a deep learning model. But what are those specific limitations? Over the next few minutes I want to explore the most frequently cited sources of non-determinism in deep learning systems, why they exist, and where they can be overcome.

Democritus: An early determinist philosopher and earlier day drinker

Let's begin with the low-level hardware operations. A major hurdle to deep learning determinism is the non associative properties of floating point arithmetic. Anyone with a CS background has probably seen something like this before:

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

## This prints False btw
print((0.7 + 0.2 + 0.1) == 1)

Some of you are already reciting the reason in your mind: 

dyadic rationals

. In simple terms, if a number can be expressed as a fraction whose denominator is a power of two, then it can be expressed exactly in finite binary representation.

If addition can't be considered deterministic, then what hope do we possibly have in making the billions of matrix operations that must take place to predict token sequences deterministic? Matrix multiplication does not suffer from the non-associative property precisely because it follows a fixed sequence of operations for which the computation pattern is consistent across most hardware. Dot products are computed in a defined order. Consider the below chunk of code from the 

excellent Two Sigma blog

 on this same topic, that showcases the comparative non-determinism of a naively implemented 

tf.reduce_sum()

 operation in TensorFlow versus one that leverages 

tf.matmul()

:

## Only runnable in TensorFlow 1.x
import tensorflow as tf
import numpy as np
N = 100
S = (1, 100000)
np.random.seed(1)
r = np.random.normal(0, 100, S).astype(np.float32)
x = tf.placeholder(tf.float32, S)
examples = {
 'reduce_sum': tf.reduce_sum(x),
 'reduce_sum_det': tf.matmul(x, tf.ones_like(x), transpose_b=True),
}
s = tf.Session()
results = {
 key: np.array([s.run(val, feed_dict={x:r}) for j in range(N)])
 for key, val in examples.items()
}
for key, val in results.items():
 print('%20s mean = %.8f max-min = %.6f' % (key, val.mean(), val.max() - val.min()))

If you don't want to switch runtimes to see the above output, you can just trust that the 

reduce_sum

 version produces inconsistencies as large as the hundredths place, while the 

matmul

 version is deterministic at least out to the millionths. I reproduced the above code block in TensorFlow 2.x calling TensorFlow 1.x syntax, but they seem to have changed the way 

tf.reduce_sum()

 gets implemented on the backend even in the naive call, as the outputs for the below are identical:

import tensorflow as tf
import numpy as np
N = 100
S = (1, 100000)
np.random.seed(1)
r = np.random.normal(0, 100, S).astype(np.float32)

def run_graph():
 tf.compat.v1.disable_eager_execution()

 x = tf.compat.v1.placeholder(tf.float32, S)
 examples = {
 'reduce_sum': tf.reduce_sum(x),
 'reduce_sum_det': tf.matmul(x, tf.ones_like(x), transpose_b=True),
 }

 with tf.compat.v1.Session() as s:
 results = {
 key: np.array([s.run(val, feed_dict={x: r}) for j in range(N)])
 for key, val in examples.items()
 }

 return results

results = run_graph()

# Print the results
for key, val in results.items():
 print('%20s mean = %.8f max-min = %.6f' % (key, val.mean(), val.max() - val.min()))

## So just by upgrading our version of TensorFlow we can achieve much greater determinism with no code changes!

If you want to dig further into the above, I would strongly advise you to check out 

A Workaround for Non-Determinism in TensorFlow

 since they provide additional example code that extends this idea from the weights to the bias terms and shows a fully deterministic training run for a neural network on the MNIST dataset.

For our purposes we've already arrived at an intermediate answer: due to the non-associative properties of floating point arithmetic, certain operations heavily leveraged by deep learning algorithms are implemented at the hardware level in a bit-wise deterministic manner while others are not.

Unfortunately we can't stop here. Simply leveraging operations like 

tf.matmul()

 instead of atomic operations like the old version of 

tf.reduce_sum()

 reduces the efficiency of deep learning models substantially as the number of GPUs is increased. For LLMs especially, where hundreds of GPUs may be used, the cost impact of a deterministic architecture would be substantial. Add to that any or all of the following:

There could be multiple GPU types the model runs on

Other operations such as attention masks, dropout, and different sampling methods also exist

In multi-GPU settings timing in synchronization can change results

Additional problems likely not considered

Even after setting fixed seeds, disabling certain stochastic optimizations, and controlling the data flow using something like 

MDS

, we're seemingly still back to square 1, with non-deterministic LLMs.

Enter our Second Protagonist: LlamaForCausalLM

LlamaForCausalLM

 has a parameter called 

do_sample

, which if set to 

False

 results in no temperature scaling, no top-k or top-p filtering, and no random sampling. In theory, the computation flow reduces to:

Forward pass -> get logits -> argmax -> pick single highest token -> rinse and repeat

And we can test this out for ourselves on Databricks by importing it from the 

transformers

 library.

## Setting up our two examples with a small model and a basic prompt:
import numpy as np
import pandas as pd
import random
import torch
from transformers import pipeline, set_seed, AutoTokenizer, LlamaForCausalLM

# HuggingFace token needed, Llama 3 is a gated repo
hf_token = dbutils.secrets.get(scope=&quot;austin_zaccor&quot;, key=&quot;hf_read_token&quot;)
model_id = &quot;meta-llama/Llama-3.2-1B&quot;

# Load tokenizer and model
tokenizer = AutoTokenizer.from_pretrained(model_id, token=hf_token)
model = LlamaForCausalLM.from_pretrained(
 model_id,
 torch_dtype=torch.float32,
 token=hf_token
)

prompt = &quot;Explain the concept of determinism in large language models.&quot;

## Non-deterministic version first:
non_deterministic_results = []
for i in range(5):
 inputs = tokenizer(prompt, return_tensors='pt')
 outputs = model.generate(
 inputs.input_ids, 
 max_length=100, 
 do_sample=True,
 temperature=None, ## unsetting temperature
 top_p=None, ## unsetting top_p
 pad_token_id=tokenizer.eos_token_id
 )
 non_deterministic_results.append(tokenizer.decode(outputs[0]))

for result in non_deterministic_results:
 print(result, '\n\n')

## Identical to the above, except do_sample=False
deterministic_results = []
for i in range(5):
 inputs = tokenizer(prompt, return_tensors='pt')
 outputs = model.generate(
 inputs.input_ids, 
 max_length=100, 
 do_sample=False,
 temperature=None, ## unsetting temperature
 top_p=None, ## unsetting top_p
 pad_token_id=tokenizer.eos_token_id
 )
 deterministic_results.append(tokenizer.decode(outputs[0]))

for result in deterministic_results:
 print(result, '\n\n')

Now we're back to something that looks promising. The first five are all over the place while the second five are all identical down to the token! In practice, this will create f



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*

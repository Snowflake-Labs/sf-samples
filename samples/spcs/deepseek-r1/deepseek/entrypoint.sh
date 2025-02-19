#!/bin/bash

# Add optional args
if [ -n "$TENSOR_PARALLEL_SIZE" ]; then
  optional_args+=("--tensor-parallel-size" "$TENSOR_PARALLEL_SIZE")
fi
if [ -n "$MAX_MODEL_LEN" ]; then
  optional_args+=("--max-model-len" "$MAX_MODEL_LEN")
fi
if [ -n "$GPU_MEMORY_UTILIZATION" ]; then
  optional_args+=("--gpu-memory-utilization" "$GPU_MEMORY_UTILIZATION")
fi

#echo "Starting vLLM server..."
python3 -m vllm.entrypoints.openai.api_server --model $MODEL --download-dir /models/ --trust-remote-code --enforce-eager "${optional_args[@]}"

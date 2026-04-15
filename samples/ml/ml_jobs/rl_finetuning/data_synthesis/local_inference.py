#!/usr/bin/env python3
"""
Local inference backend using vLLM for Qwen3-235B-A22B model.
Supports tensor parallelism across multiple GPUs.
"""

import logging
from typing import List, Optional, Union

logger = logging.getLogger(__name__)

# Global LLM instance (initialized lazily)
_llm_instance = None
_current_model_name = None


def get_llm(
    model_name: str = "Qwen/Qwen3-235B-A22B-Instruct-2507",
    tensor_parallel_size: int = 4,
    dtype: str = "bfloat16",
    max_model_len: int = 8192,  # Limit context length to save KV cache memory
    gpu_memory_utilization: float = 0.90,
):
    """
    Get or create the vLLM instance.
    
    Args:
        model_name: HuggingFace model name or path
        tensor_parallel_size: Number of GPUs for tensor parallelism
        dtype: Data type for model weights (bfloat16, float16, auto)
        max_model_len: Maximum sequence length (None for auto)
        gpu_memory_utilization: Fraction of GPU memory to use
    
    Returns:
        vLLM LLM instance
    """
    global _llm_instance, _current_model_name
    
    if _llm_instance is not None and _current_model_name == model_name:
        return _llm_instance
    
    # Import vLLM here to avoid import errors when using Cortex backend
    from vllm import LLM
    
    logger.info(f"Initializing vLLM with model: {model_name}")
    logger.info(f"  Tensor parallel size: {tensor_parallel_size}")
    logger.info(f"  Data type: {dtype}")
    logger.info(f"  GPU memory utilization: {gpu_memory_utilization}")
    
    _llm_instance = LLM(
        model=model_name,
        tensor_parallel_size=tensor_parallel_size,
        trust_remote_code=True,
        dtype=dtype,
        max_model_len=max_model_len,
        gpu_memory_utilization=gpu_memory_utilization,
    )
    _current_model_name = model_name
    
    logger.info("vLLM model loaded successfully")
    return _llm_instance


def format_chat_prompt(prompt: str, system_prompt: Optional[str] = None) -> str:
    """
    Format prompt using Qwen3 chat template.
    
    Qwen3 uses a specific format:
    <|im_start|>system
    {system}<|im_end|>
    <|im_start|>user
    {user}<|im_end|>
    <|im_start|>assistant
    
    For non-thinking mode, add /no_think to disable thinking.
    """
    parts = []
    
    if system_prompt:
        parts.append(f"<|im_start|>system\n{system_prompt}<|im_end|>")
    
    # Add /no_think to disable thinking mode in Qwen3
    user_content = f"{prompt}\n\n/no_think"
    parts.append(f"<|im_start|>user\n{user_content}<|im_end|>")
    parts.append("<|im_start|>assistant\n")
    
    return "\n".join(parts)


def complete_local(
    prompt: str,
    system_prompt: Optional[str] = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    top_p: float = 0.9,
    stop: Optional[List[str]] = None,
    use_chat_template: bool = True,
) -> str:
    """
    Generate completion for a single prompt.
    
    Args:
        prompt: Input prompt text (user message)
        system_prompt: System prompt for chat template
        max_tokens: Maximum tokens to generate
        temperature: Sampling temperature
        top_p: Top-p (nucleus) sampling
        stop: Stop sequences
        use_chat_template: Whether to use Qwen3 chat template
    
    Returns:
        Generated text
    """
    from vllm import SamplingParams
    
    llm = get_llm()
    
    # Format prompt with chat template if needed
    if use_chat_template:
        formatted_prompt = format_chat_prompt(prompt, system_prompt)
    else:
        # Fallback: concatenate system and user prompts
        if system_prompt:
            formatted_prompt = f"{system_prompt}\n\n{prompt}"
        else:
            formatted_prompt = prompt
    
    # Add stop tokens for chat format
    stop_tokens = stop or []
    if use_chat_template:
        stop_tokens = list(stop_tokens) + ["<|im_end|>", "<|endoftext|>"]
    
    sampling_params = SamplingParams(
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        stop=stop_tokens if stop_tokens else None,
    )
    
    outputs = llm.generate([formatted_prompt], sampling_params)
    
    if outputs and outputs[0].outputs:
        response = outputs[0].outputs[0].text
        # Clean up any remaining special tokens
        response = response.replace("<|im_end|>", "").replace("<|endoftext|>", "")
        return response.strip()
    return ""


def complete_batch(
    prompts: List[str],
    system_prompt: Optional[str] = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    top_p: float = 0.9,
    stop: Optional[List[str]] = None,
    use_chat_template: bool = True,
) -> List[str]:
    """
    Generate completions for a batch of prompts.
    
    This is more efficient than calling complete_local() multiple times
    as vLLM can process batches in parallel.
    
    Args:
        prompts: List of input prompts (user messages)
        system_prompt: System prompt for chat template (same for all)
        max_tokens: Maximum tokens to generate per prompt
        temperature: Sampling temperature
        top_p: Top-p (nucleus) sampling
        stop: Stop sequences
        use_chat_template: Whether to use Qwen3 chat template
    
    Returns:
        List of generated texts
    """
    from vllm import SamplingParams
    
    if not prompts:
        return []
    
    llm = get_llm()
    
    # Format prompts with chat template if needed
    if use_chat_template:
        formatted_prompts = [format_chat_prompt(p, system_prompt) for p in prompts]
    else:
        # Fallback: concatenate system and user prompts
        if system_prompt:
            formatted_prompts = [f"{system_prompt}\n\n{p}" for p in prompts]
        else:
            formatted_prompts = prompts
    
    # Add stop tokens for chat format
    stop_tokens = stop or []
    if use_chat_template:
        stop_tokens = list(stop_tokens) + ["<|im_end|>", "<|endoftext|>"]
    
    sampling_params = SamplingParams(
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        stop=stop_tokens if stop_tokens else None,
    )
    
    outputs = llm.generate(formatted_prompts, sampling_params)
    
    results = []
    for output in outputs:
        if output.outputs:
            response = output.outputs[0].text
            # Clean up any remaining special tokens
            response = response.replace("<|im_end|>", "").replace("<|endoftext|>", "")
            results.append(response.strip())
        else:
            results.append("")
    
    return results


def initialize_model(
    model_name: str = "Qwen/Qwen3-235B-A22B-Instruct-2507",
    tensor_parallel_size: int = 4,
    dtype: str = "bfloat16",
    max_model_len: int = 8192,  # Limit context length to save KV cache memory
    gpu_memory_utilization: float = 0.90,
):
    """
    Pre-initialize the model. Call this at startup to avoid lazy loading.
    
    Args:
        model_name: HuggingFace model name or path
        tensor_parallel_size: Number of GPUs for tensor parallelism
        dtype: Data type for model weights
        max_model_len: Maximum sequence length
        gpu_memory_utilization: Fraction of GPU memory to use
    """
    global _llm_instance, _current_model_name
    
    # Import vLLM here to avoid import errors when using Cortex backend
    from vllm import LLM
    
    logger.info(f"Pre-initializing vLLM with model: {model_name}")
    logger.info(f"  Tensor parallel size: {tensor_parallel_size}")
    logger.info(f"  Data type: {dtype}")
    logger.info(f"  GPU memory utilization: {gpu_memory_utilization}")
    
    _llm_instance = LLM(
        model=model_name,
        tensor_parallel_size=tensor_parallel_size,
        trust_remote_code=True,
        dtype=dtype,
        max_model_len=max_model_len,
        gpu_memory_utilization=gpu_memory_utilization,
    )
    _current_model_name = model_name
    
    logger.info("vLLM model loaded successfully")


def cleanup():
    """Clean up the LLM instance and free GPU memory."""
    global _llm_instance, _current_model_name
    
    if _llm_instance is not None:
        logger.info("Cleaning up vLLM instance")
        del _llm_instance
        _llm_instance = None
        _current_model_name = None
        
        # Force CUDA memory cleanup
        try:
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except ImportError:
            pass


if __name__ == "__main__":
    # Test the local inference
    import argparse
    
    parser = argparse.ArgumentParser(description="Test local vLLM inference")
    parser.add_argument(
        "--model",
        type=str,
        default="Qwen/Qwen3-235B-A22B-Instruct-2507",
        help="Model name",
    )
    parser.add_argument(
        "--tensor-parallel",
        type=int,
        default=4,
        help="Tensor parallel size",
    )
    parser.add_argument(
        "--prompt",
        type=str,
        default="Hello, how are you today?",
        help="Test prompt",
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    print(f"Testing vLLM with model: {args.model}")
    print(f"Tensor parallel size: {args.tensor_parallel}")
    
    # Initialize
    initialize_model(
        model_name=args.model,
        tensor_parallel_size=args.tensor_parallel,
    )
    
    # Test single completion
    print(f"\nPrompt: {args.prompt}")
    response = complete_local(args.prompt, max_tokens=100, temperature=0.7)
    print(f"Response: {response}")
    
    # Cleanup
    cleanup()
    print("\nTest complete!")

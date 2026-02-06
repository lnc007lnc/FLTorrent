from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import logging

logger = logging.getLogger(__name__)


def get_rnn(model_config, input_shape):
    from federatedscope.nlp.model.rnn import LSTM
    # check the task
    # input_shape: (batch_size, seq_len, hidden) or (seq_len, hidden)
    if model_config.type == 'lstm':
        model = LSTM(
            in_channels=input_shape[-2]
            if not model_config.in_channels else model_config.in_channels,
            hidden=model_config.hidden,
            out_channels=model_config.out_channels,
            embed_size=model_config.embed_size,
            dropout=model_config.dropout)
    else:
        raise ValueError(f'No model named {model_config.type}!')

    return model


def get_transformer(model_config, input_shape):
    """
    Build a transformer model from HuggingFace, with optional LoRA support.

    Args:
        model_config: Configuration object with the following attributes:
            - type: Model path in format "{model_name}@transformers"
            - task: Task type (SequenceClassification, QuestionAnswering, etc.)
            - out_channels: Number of output labels
            - use_lora: Whether to use LoRA (default: False)
            - lora_r: LoRA rank (default: 8)
            - lora_alpha: LoRA alpha scaling (default: 16)
            - lora_dropout: LoRA dropout (default: 0.1)
            - lora_target_modules: Target modules for LoRA (default: auto-detect)
        input_shape: Input shape (not used for transformers)

    Returns:
        model: The transformer model, optionally wrapped with LoRA
    """
    from transformers import AutoModelForPreTraining, \
        AutoModelForQuestionAnswering, AutoModelForSequenceClassification, \
        AutoModelForTokenClassification, AutoModelWithLMHead, AutoModel, \
        AutoModelForCausalLM

    model_func_dict = {
        'pretraining': AutoModelForPreTraining,
        'questionanswering': AutoModelForQuestionAnswering,
        'sequenceclassification': AutoModelForSequenceClassification,
        'tokenclassification': AutoModelForTokenClassification,
        'withlmhead': AutoModelWithLMHead,
        'causallm': AutoModelForCausalLM,
        'auto': AutoModel
    }

    task = model_config.task.lower()
    assert task in model_func_dict, \
        f'model_config.task should be in {model_func_dict.keys()} ' \
        f'when using pre_trained transformer model, got {task}'

    # Parse model path
    path, _ = model_config.type.split('@')

    # Load base model
    logger.info(f"Loading transformer model: {path}")

    # Handle different tasks with appropriate arguments
    if task in ['causallm', 'withlmhead', 'pretraining']:
        model = model_func_dict[task].from_pretrained(path)
    else:
        model = model_func_dict[task].from_pretrained(
            path, num_labels=model_config.out_channels)

    # Apply LoRA if enabled
    use_lora = getattr(model_config, 'use_lora', False)
    if use_lora:
        model = apply_lora(model, model_config, path)

    return model


def apply_lora(model, model_config, model_path):
    """
    Apply LoRA (Low-Rank Adaptation) to a transformer model.

    Args:
        model: The base transformer model
        model_config: Configuration object with LoRA settings
        model_path: Path to the model (for auto-detecting target modules)

    Returns:
        model: The model wrapped with LoRA adapters
    """
    try:
        from peft import get_peft_model, LoraConfig, TaskType
    except ImportError:
        raise ImportError(
            "PEFT library is required for LoRA support. "
            "Please install it with: pip install peft"
        )

    # Get LoRA configuration from model_config
    lora_r = getattr(model_config, 'lora_r', 8)
    lora_alpha = getattr(model_config, 'lora_alpha', 16)
    lora_dropout = getattr(model_config, 'lora_dropout', 0.1)
    lora_target_modules = getattr(model_config, 'lora_target_modules', None)

    # Auto-detect target modules based on model architecture
    if lora_target_modules is None or len(lora_target_modules) == 0:
        lora_target_modules = _get_default_target_modules(model_path)

    # Map task to PEFT TaskType
    task = model_config.task.lower()
    task_type_map = {
        'sequenceclassification': TaskType.SEQ_CLS,
        'tokenclassification': TaskType.TOKEN_CLS,
        'questionanswering': TaskType.QUESTION_ANS,
        'causallm': TaskType.CAUSAL_LM,
        'withlmhead': TaskType.CAUSAL_LM,
        'pretraining': TaskType.CAUSAL_LM,
        'auto': TaskType.FEATURE_EXTRACTION,
    }
    peft_task_type = task_type_map.get(task, TaskType.FEATURE_EXTRACTION)

    # Create LoRA configuration
    lora_config = LoraConfig(
        r=lora_r,
        lora_alpha=lora_alpha,
        target_modules=lora_target_modules,
        lora_dropout=lora_dropout,
        bias="none",
        task_type=peft_task_type,
    )

    logger.info(f"Applying LoRA with config: r={lora_r}, alpha={lora_alpha}, "
                f"dropout={lora_dropout}, target_modules={lora_target_modules}")

    # Apply LoRA
    model = get_peft_model(model, lora_config)

    # Log trainable parameters
    trainable_params, all_params = _count_parameters(model)
    logger.info(f"LoRA applied: trainable params: {trainable_params:,} || "
                f"all params: {all_params:,} || "
                f"trainable%: {100 * trainable_params / all_params:.4f}%")

    return model


def _get_default_target_modules(model_path):
    """
    Get default LoRA target modules based on model architecture.

    Args:
        model_path: HuggingFace model path

    Returns:
        list: Target module names for LoRA
    """
    model_path_lower = model_path.lower()

    # LLaMA, Qwen, Mistral family
    if any(name in model_path_lower for name in ['llama', 'qwen', 'mistral', 'yi']):
        return ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]

    # GPT-2, GPT-Neo, GPT-J family
    elif any(name in model_path_lower for name in ['gpt2', 'gpt-neo', 'gpt-j', 'gptj']):
        return ["c_attn", "c_proj", "c_fc"]

    # BERT, RoBERTa, DistilBERT family
    elif any(name in model_path_lower for name in ['bert', 'roberta', 'distilbert']):
        return ["query", "key", "value", "dense"]

    # T5, FLAN-T5 family
    elif any(name in model_path_lower for name in ['t5', 'flan']):
        return ["q", "k", "v", "o", "wi", "wo"]

    # Falcon family
    elif 'falcon' in model_path_lower:
        return ["query_key_value", "dense", "dense_h_to_4h", "dense_4h_to_h"]

    # Phi family
    elif 'phi' in model_path_lower:
        return ["q_proj", "k_proj", "v_proj", "dense", "fc1", "fc2"]

    # BLOOM family
    elif 'bloom' in model_path_lower:
        return ["query_key_value", "dense", "dense_h_to_4h", "dense_4h_to_h"]

    # OPT family
    elif 'opt' in model_path_lower:
        return ["q_proj", "k_proj", "v_proj", "out_proj", "fc1", "fc2"]

    # Default: common attention layers
    else:
        logger.warning(f"Unknown model architecture for {model_path}, "
                       f"using default target modules")
        return ["q_proj", "v_proj"]


def _count_parameters(model):
    """
    Count trainable and total parameters in a model.

    Args:
        model: PyTorch model

    Returns:
        tuple: (trainable_params, all_params)
    """
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    all_params = sum(p.numel() for p in model.parameters())
    return trainable_params, all_params

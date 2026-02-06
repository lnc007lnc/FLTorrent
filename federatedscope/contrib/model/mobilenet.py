"""MobileNetV2 model for FederatedScope.

Provides both ImageNet and CIFAR-optimized versions of MobileNetV2.
CIFAR version uses stride=1 in first conv to preserve spatial resolution for 32x32 inputs.
"""
from federatedscope.register import register_model
import torch.nn as nn
import torch


class ConvBNReLU(nn.Sequential):
    """Convolution + BatchNorm + ReLU6 block."""
    def __init__(self, in_planes, out_planes, kernel_size=3, stride=1, groups=1):
        padding = (kernel_size - 1) // 2
        super(ConvBNReLU, self).__init__(
            nn.Conv2d(in_planes, out_planes, kernel_size, stride, padding, groups=groups, bias=False),
            nn.BatchNorm2d(out_planes),
            nn.ReLU6(inplace=True)
        )


class InvertedResidual(nn.Module):
    """Inverted Residual block (MobileNetV2 building block)."""
    def __init__(self, inp, oup, stride, expand_ratio):
        super(InvertedResidual, self).__init__()
        self.stride = stride
        assert stride in [1, 2]

        hidden_dim = int(round(inp * expand_ratio))
        self.use_res_connect = self.stride == 1 and inp == oup

        layers = []
        if expand_ratio != 1:
            # Pointwise expansion
            layers.append(ConvBNReLU(inp, hidden_dim, kernel_size=1))
        layers.extend([
            # Depthwise convolution
            ConvBNReLU(hidden_dim, hidden_dim, stride=stride, groups=hidden_dim),
            # Pointwise linear projection
            nn.Conv2d(hidden_dim, oup, 1, 1, 0, bias=False),
            nn.BatchNorm2d(oup),
        ])
        self.conv = nn.Sequential(*layers)

    def forward(self, x):
        if self.use_res_connect:
            return x + self.conv(x)
        else:
            return self.conv(x)


class MobileNetV2CIFAR(nn.Module):
    """MobileNetV2 optimized for CIFAR/CINIC-10 (32x32 images).

    Key differences from ImageNet version:
    - First conv uses stride=1 instead of stride=2
    - Removes one stride=2 layer to maintain spatial resolution
    - Final feature map is 4x4 instead of 7x7

    For 32x32 input:
    - Layer progression: 32 -> 32 -> 16 -> 8 -> 4 -> 1 (via adaptive avg pool)
    """
    def __init__(self, num_classes=10, width_mult=1.0):
        super(MobileNetV2CIFAR, self).__init__()

        input_channel = 32
        last_channel = 1280

        # CIFAR-optimized inverted residual settings
        # t: expansion factor, c: output channels, n: repeat count, s: stride
        # Reduced strides compared to ImageNet version
        inverted_residual_setting = [
            # t, c, n, s
            [1, 16, 1, 1],   # stride 1 (ImageNet: 1)
            [6, 24, 2, 2],   # stride 2: 32 -> 16 (ImageNet: 2)
            [6, 32, 3, 2],   # stride 2: 16 -> 8 (ImageNet: 2)
            [6, 64, 4, 2],   # stride 2: 8 -> 4 (ImageNet: 2)
            [6, 96, 3, 1],   # stride 1 (ImageNet: 1)
            [6, 160, 3, 1],  # stride 1 (ImageNet: 2) - CHANGED to preserve 4x4
            [6, 320, 1, 1],  # stride 1 (ImageNet: 1)
        ]

        # Build first layer - stride=1 for CIFAR (vs stride=2 for ImageNet)
        input_channel = int(input_channel * width_mult)
        self.last_channel = int(last_channel * max(1.0, width_mult))

        # First conv: stride=1 to preserve 32x32
        features = [ConvBNReLU(3, input_channel, stride=1)]

        # Build inverted residual blocks
        for t, c, n, s in inverted_residual_setting:
            output_channel = int(c * width_mult)
            for i in range(n):
                stride = s if i == 0 else 1
                features.append(InvertedResidual(input_channel, output_channel, stride, expand_ratio=t))
                input_channel = output_channel

        # Build last several layers
        features.append(ConvBNReLU(input_channel, self.last_channel, kernel_size=1))

        self.features = nn.Sequential(*features)

        # Classifier
        self.classifier = nn.Sequential(
            nn.Dropout(0.2),
            nn.Linear(self.last_channel, num_classes),
        )

        # Weight initialization
        self._initialize_weights()

    def _initialize_weights(self):
        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight, mode='fan_out')
                if m.bias is not None:
                    nn.init.zeros_(m.bias)
            elif isinstance(m, nn.BatchNorm2d):
                nn.init.ones_(m.weight)
                nn.init.zeros_(m.bias)
            elif isinstance(m, nn.Linear):
                nn.init.normal_(m.weight, 0, 0.01)
                nn.init.zeros_(m.bias)

    def forward(self, x):
        x = self.features(x)
        # Adaptive pooling for any input size
        x = nn.functional.adaptive_avg_pool2d(x, (1, 1))
        x = torch.flatten(x, 1)
        x = self.classifier(x)
        return x


def MobileNetV2(num_classes=10, pretrained=False, cifar_optimized=True):
    """Create MobileNetV2 model.

    Args:
        num_classes: Number of output classes
        pretrained: Whether to use pretrained weights (default: False for FL)
        cifar_optimized: If True, use CIFAR-optimized architecture (stride=1 first conv)
                        If False, use original ImageNet architecture

    Returns:
        MobileNetV2 model with modified classifier
    """
    if cifar_optimized and not pretrained:
        # Use CIFAR-optimized version for small images (32x32)
        return MobileNetV2CIFAR(num_classes=num_classes)
    else:
        # Use original torchvision version for ImageNet or pretrained
        from torchvision.models import mobilenet_v2
        if pretrained:
            model = mobilenet_v2(weights='IMAGENET1K_V1')
        else:
            model = mobilenet_v2(weights=None)

        # Modify classifier for custom num_classes
        model.classifier[1] = nn.Linear(model.last_channel, num_classes)
        return model


def call_mobilenet(model_config, local_data):
    """Model builder function for registration."""
    model_type = model_config.type.lower()

    if 'mobilenet' in model_type:
        num_classes = model_config.out_channels if hasattr(model_config, 'out_channels') else 10
        pretrained = getattr(model_config, 'pretrained', False)
        # Default to CIFAR-optimized for FL (training from scratch on small images)
        cifar_optimized = getattr(model_config, 'cifar_optimized', True)
        return MobileNetV2(num_classes=num_classes, pretrained=pretrained, cifar_optimized=cifar_optimized)

    return None


register_model('mobilenet', call_mobilenet)

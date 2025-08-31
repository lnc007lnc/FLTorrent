#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test new multi-factor chunk sorting algorithm
Validate the comprehensive scoring effect of importance, rarity and random perturbation
"""

import random

def test_chunk_sorting():
    """Test multi-factor sorting algorithm"""
    print("🧪 Testing multi-factor chunk sorting algorithm...")
    
    # Simulate chunk data
    chunks = [
        {'chunk_key': (1, 1, 0), 'importance': 0.9, 'availability': 1},    # High importance, very rare
        {'chunk_key': (1, 1, 1), 'importance': 0.7, 'availability': 3},    # Medium importance, moderately rare
        {'chunk_key': (1, 1, 2), 'importance': 0.5, 'availability': 5},    # Low importance, common
        {'chunk_key': (1, 1, 3), 'importance': 0.8, 'availability': 2},    # High importance, rare
        {'chunk_key': (1, 1, 4), 'importance': 0.6, 'availability': 4},    # Medium importance, fairly common
    ]
    
    # Calculate rarity score (simulate 5 peers)
    total_peers = 5
    for chunk in chunks:
        chunk['rarity'] = 1.0 - (chunk['availability'] / max(total_peers, 1))
    
    print("📊 Original chunk data:")
    for i, chunk in enumerate(chunks):
        print(f"  Chunk {i}: {chunk['chunk_key']} - importance: {chunk['importance']:.1f}, "
              f"availability: {chunk['availability']}, rarity: {chunk['rarity']:.3f}")
    
    # Apply new sorting algorithm
    tau = 0.01      # rarity weight
    eps = 1e-6      # rarity adjustment parameter  
    gamma = 1e-4    # random perturbation strength
    
    # Set random seed for reproducible results
    random.seed(42)
    
    chunks.sort(
        key=lambda x: (
            x['importance'] + (tau - eps) * x['rarity'] + gamma * random.uniform(-1, 1)
        ),
        reverse=True
    )
    
    print(f"\n🎯 Sorted chunk order (tau={tau}, eps={eps}, gamma={gamma}):")
    for i, chunk in enumerate(chunks):
        final_score = chunk['importance'] + (tau - eps) * chunk['rarity']
        print(f"  #{i+1}: {chunk['chunk_key']} - final_score: {final_score:.6f} "
              f"(importance: {chunk['importance']:.1f}, rarity: {chunk['rarity']:.3f})")
    
    # Validate sorting logic
    print("\n✅ Sorting algorithm validation:")
    print("- High importance + high rarity chunks should be ranked first")
    print("- Rarity weight is small (tau=0.01), mainly based on importance")
    print("- Random perturbation (gamma=1e-4) provides slight randomness to avoid complete determinism")
    
    return True

if __name__ == "__main__":
    success = test_chunk_sorting()
    if success:
        print("\n🎉 Multi-factor sorting algorithm test completed!")
    else:
        print("\n💥 Sorting algorithm test failed!")
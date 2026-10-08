import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

export function loadPipelineConfig(path = process.env.PIPELINE_CONFIG || 'config/pipeline.json') {
  return JSON.parse(readFileSync(resolve(path), 'utf8'));
}

export function getDirectionPatterns(config = loadPipelineConfig()) {
  return {
    positive: new RegExp(config.direction.positive_regex.join('|'), 'i'),
    negative: new RegExp(config.direction.negative_regex.join('|'), 'i'),
  };
}

export function classifyDirection(text, config = loadPipelineConfig()) {
  const { positive, negative } = getDirectionPatterns(config);
  const positiveCount = (String(text || '').match(new RegExp(positive.source, 'gi')) || []).length;
  const negativeCount = (String(text || '').match(new RegExp(negative.source, 'gi')) || []).length;
  if (positiveCount > negativeCount) return { direction: 'bullish', confidence: Math.min(0.95, 0.72 + (positiveCount - negativeCount) * 0.08), positiveCount, negativeCount };
  if (negativeCount > positiveCount) return { direction: 'bearish', confidence: Math.min(0.95, 0.72 + (negativeCount - positiveCount) * 0.08), positiveCount, negativeCount };
  return { direction: 'neutral', confidence: 0.55, positiveCount, negativeCount };
}

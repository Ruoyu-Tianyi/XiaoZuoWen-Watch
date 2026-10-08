function rawTopic(topic) {
  if (!topic?.raw_json) return topic || {};
  try { return { ...JSON.parse(topic.raw_json), ...topic }; }
  catch { return topic || {}; }
}

export function topicIsEligible(topic) {
  const raw = rawTopic(topic);
  const content = String(raw.content || '').trim();
  const images = Array.isArray(raw.images) ? raw.images : [];
  const files = Array.isArray(raw.files) ? raw.files : [];
  return content.length > 0 && images.length === 0 && files.length === 0;
}

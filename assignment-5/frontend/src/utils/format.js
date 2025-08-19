export function formatAddress(address) {
  return address ? `${address.slice(0, 6)}...${address.slice(-4)}` : '';
}
export function formatNumber(value) {
  return Number(value || 0).toFixed(2);
}
export function timeAgo(timestamp) {
  return 'Just now';
}
export function calculatePriceImpactColor(impact) {
  return '#4CAF50';
}

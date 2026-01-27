const lcgrand = (seed = 1) => (a = 0, b = 1) =>
  a +
  (Math.abs(b - a) *
    (Math.imul(48271, (seed = Math.imul(214013, seed) + 2531011)) &
      0x7fffffff)) /
    0x7fffffff;

const pol2vec = (l, a = 0) => [l * Math.cos(a), l * Math.sin(a)];
const vecmag = ([x, y]) => Math.hypot(x, y);
const vecnorm = ([x, y], l = vecmag([x, y])) => (l === 0 ? [0, 0] : [x / l, y / l]);

const trapezoidalWave = (l, a, b) => {
  const s = Math.max(a, l - b);
  return (t) => {
    if (t < a) return Math.max(0, t / a);
    if (t > s) return Math.max(0, 1 - (t - s) / (l - s));
    return 1;
  };
};

const _cycle = (x, n) => ((x % n) + n) % n;
const _mirror = (x, n, r) => (x < r ? n + x : x > n - r ? x - n : x);
const cycleBounds = ([x, y], [w, h], r) => {
  const tx = _cycle(x, w);
  const ty = _cycle(y, h);
  return [
    [tx, ty],
    [_mirror(tx, w, r), _mirror(ty, h, r)],
  ];
};

function easeOutCubic(t) {
  return --t * t * t + 1;
}

const animateFadeInOut = (World, idx, duration, ease = easeOutCubic) => {
  const direction = World.tStop <= World.t ? 'out' : 'in';
  const startT = direction === 'in' ? World.tStart : World.tStop;
  const t = startT + (2 / 3 * duration * idx) / World.n;
  const fade = (1 / 3) * duration;
  let progress;
  if (direction === 'in') {
    progress = (fade + t - World.t) / fade || 0;
  } else {
    progress = (World.t - t) / fade || 1;
  }
  return ease(1 - Math.max(0, Math.min(progress, 1)));
};

const FAKE_WORDS = [5, 3, 4, 4, 2, 4, 7, 6, 8, 6, 3, 1, 6];

function makeWordDistribution(line, em, space) {
  let marker = 0;
  let i = 0;
  let wordslen = 0;
  const chunks = [];
  do {
    const end = Math.min(line, marker + FAKE_WORDS[i++ % FAKE_WORDS.length] * em);
    wordslen += end - marker;
    chunks.push([marker, (marker = end)]);
  } while ((marker += space) < line);
  chunks[chunks.length - 1][1] = line;
  return (t) => {
    const w = t * wordslen;
    let m = 0;
    for (const [s, e] of chunks) {
      const len = e - s;
      if (m < w && w <= m + len) return s + w - m;
      m += len;
    }
    return 0;
  };
}

class SpoilerPainterWorklet {
  static get contextOptions() {
    return { alpha: true };
  }

  static get inputProperties() {
    return ['--t', '--t-stop', '--fade', '--gap', '--accent', '--words', '--density'];
  }

  paint(ctx, size, props) {
    const rand = lcgrand(4011505);
    const dprx = 1;
    const accentArr = (props.get('--accent')?.toString() || '0 0% 0%').split(' ');
    const mimicW = props.get('--words')?.toString() === 'true';
    const [hgap, vgap] = (props.get('--gap') || '0px 0px').toString().split(' ').map(parseFloat);
    const density = parseFloat(props.get('--density')) || 0.08;
    const fadeDur = parseFloat(props.get('--fade')) || 0;
    const width = size.width / dprx;
    const height = size.height / dprx;
    const World = {
      t: parseFloat(props.get('--t') ?? 0),
      tStop: parseFloat(props.get('--t-stop') ?? Infinity),
      tStart: 0,
      n: Math.round(Math.min(5000, density * (width - 2 * hgap) * (height - 2 * vgap))),
    };

    ctx.clearRect(0, 0, size.width, size.height);

    const wordDist = mimicW
      ? makeWordDistribution(width, Math.max(12, height / 4), Math.max(12, height / 4) / 4)
      : (x) => x * (width - 2 * hgap);

    for (let i = 0; i < World.n; i++) {
      const x0 = hgap + wordDist(rand());
      const y0 = vgap + rand() * (height - 2 * vgap);
      const vmag = rand(2, 12);
      const size0 = rand(1, 1.5);
      const angle = rand(0, Math.PI * 2);
      const [vx0, vy0] = pol2vec(vmag, angle);
      const shape = rand() > 0.5 ? 'square' : 'circle';
      const lifetime = rand(0.3, 1.5);
      const respawn = rand(0, 1);
      const visFn = trapezoidalWave(lifetime, 0.15, 0.3);
      const phase = rand(0, lifetime + respawn);
      const cant =
        Math.floor((World.tStop + phase) / (lifetime + respawn)) <
        Math.floor((World.t + phase) / (lifetime + respawn));
      if (cant) continue;
      const t = Math.min(lifetime, (World.t + phase) % (lifetime + respawn));
      const fade = animateFadeInOut(World, i, fadeDur);
      const alpha = fade * (1 - t / lifetime);
      const size = fade * (size0 * visFn(t));
      const lightDir = parseInt(accentArr[2]) > 50 ? -1 : 1;
      const lightL = Math.floor(
        Math.max(0, Math.min(100, parseInt(accentArr[2]) + lightDir * rand(0, 30)))
      );

      for (const [wx, wy] of cycleBounds([x0 + vx0 * t, y0 + vy0 * t], [width, height], size / 2)) {
        ctx.beginPath();
        ctx.fillStyle = `hsl(${accentArr[0]} ${accentArr[1]} ${lightL}% / ${
          Math.round(alpha * 100)
        }%)`;
        if (shape === 'square') {
          ctx.rect(dprx * wx, dprx * wy, dprx * size, dprx * size);
        } else {
          ctx.arc(dprx * wx, dprx * wy, dprx * size / 2, 0, Math.PI * 2);
        }
        ctx.closePath();
        ctx.fill();
      }
    }
  }
}

if (typeof registerPaint !== 'undefined') {
  registerPaint('spoiler', SpoilerPainterWorklet);
}

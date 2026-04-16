import './ui.css';

export function Card(props: { title?: string; children: React.ReactNode; right?: React.ReactNode }) {
  return (
    <section className="card">
      {(props.title || props.right) && (
        <div className="card-hd">
          <h2 className="card-title">{props.title}</h2>
          <div>{props.right}</div>
        </div>
      )}
      <div className="card-bd">{props.children}</div>
    </section>
  );
}

export function Pill(props: { label: string; tone?: 'neutral' | 'good' | 'bad' | 'warn' }) {
  const tone = props.tone ?? 'neutral';
  return <span className={`pill pill-${tone}`}>{props.label}</span>;
}

export function Button(props: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: 'primary' | 'ghost' }) {
  const v = props.variant ?? 'primary';
  return (
    <button {...props} className={`btn btn-${v} ${props.className ?? ''}`}>
      {props.children}
    </button>
  );
}

export function DangerButton(props: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button {...props} className={`btn btn-danger ${props.className ?? ''}`}>
      {props.children}
    </button>
  );
}

export function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={`input ${props.className ?? ''}`} />;
}

export function Select(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return <select {...props} className={`select ${props.className ?? ''}`} />;
}

export function Small(props: React.HTMLAttributes<HTMLSpanElement> & { children: React.ReactNode }) {
  const { className, ...rest } = props;
  return (
    <span {...rest} className={`small ${className ?? ''}`}>
      {props.children}
    </span>
  );
}

import { useId, useMemo, useState } from 'react';
import { Button, Small } from './Ui';
import './dropzone.css';

export function Dropzone(props: {
  value: File | null;
  onChange: (f: File | null) => void;
  accept?: string;
}) {
  const id = useId();
  const [dragOver, setDragOver] = useState(false);

  const label = useMemo(() => {
    if (!props.value) return 'Drop a Discord export here, or browse';
    return props.value.name;
  }, [props.value]);

  return (
    <div
      className={dragOver ? 'dz dz-over' : 'dz'}
      onDragEnter={(e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragOver(true);
      }}
      onDragOver={(e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragOver(true);
      }}
      onDragLeave={(e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragOver(false);
      }}
      onDrop={(e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragOver(false);
        const f = e.dataTransfer.files?.[0] ?? null;
        props.onChange(f);
      }}
    >
      <input
        id={id}
        className="dz-input"
        type="file"
        accept={props.accept}
        onChange={(e) => props.onChange(e.target.files?.[0] ?? null)}
      />
      <div className="dz-inner">
        <div className="dz-title">{label}</div>
        <Small className="dz-sub">We store the raw text and keep historical runs for debugging and P&amp;L review.</Small>
        <div className="dz-actions">
          <label htmlFor={id} className="btn btn-ghost">
            Browse…
          </label>
          {props.value && (
            <Button variant="ghost" type="button" onClick={() => props.onChange(null)}>
              Clear
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}


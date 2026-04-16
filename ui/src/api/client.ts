export type ApiError = { error: string; [k: string]: unknown };

async function parseJson<T>(res: Response): Promise<T> {
  const text = await res.text();
  try {
    return JSON.parse(text) as T;
  } catch {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { error: `Non-JSON response (${res.status})`, raw: text } as any as T;
  }
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path, { method: 'GET' });
  return await parseJson<T>(res);
}

export async function apiPostForm<T>(path: string, form: FormData): Promise<T> {
  const res = await fetch(path, { method: 'POST', body: form });
  return await parseJson<T>(res);
}


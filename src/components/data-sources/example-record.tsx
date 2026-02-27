interface ExampleRecordProps {
  record: Record<string, unknown>;
}

export function ExampleRecord({ record }: ExampleRecordProps) {
  const json = JSON.stringify(record, null, 2);

  return (
    <pre className="overflow-x-auto rounded-md border border-border bg-background/80 p-3 font-mono text-xs leading-relaxed text-muted-foreground">
      {json.split("\n").map((line, i) => {
        // Colorize keys and values
        const keyMatch = line.match(/^(\s*)"([^"]+)":/);
        if (keyMatch) {
          const [, indent, key] = keyMatch;
          const rest = line.slice(keyMatch[0].length);
          return (
            <span key={i}>
              {indent}&quot;<span className="text-primary">{key}</span>&quot;:{rest}
              {"\n"}
            </span>
          );
        }
        return (
          <span key={i}>
            {line}
            {"\n"}
          </span>
        );
      })}
    </pre>
  );
}

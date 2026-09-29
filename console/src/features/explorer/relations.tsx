// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Button } from "../../components/ui/button";
import {
  nodeLabel,
  referenceLabel,
  type RecordedRelation,
} from "../explorer-model";

export function RelationList({
  relations,
  select,
}: {
  relations: RecordedRelation[];
  select: (id: string) => void;
}) {
  const [limit, setLimit] = useState(100);
  if (!relations.length)
    return <p>No relationship evidence is recorded on this page.</p>;
  return (
    <>
      <ul>
        {relations
          .slice(0, limit)
          .map(({ record, relation, source, target, evidenceIds }) => (
            <li key={record.id}>
              {source ? (
                <button type="button" onClick={() => select(source.id)}>
                  {nodeLabel(source)}
                </button>
              ) : (
                <span>
                  {referenceLabel(record.source)}{" "}
                  <small>· not loaded on this page</small>
                </span>
              )}
              {" → "}
              {relation}
              {" → "}
              {target ? (
                <button type="button" onClick={() => select(target.id)}>
                  {nodeLabel(target)}
                </button>
              ) : (
                <span>
                  {referenceLabel(record.target)}{" "}
                  <small>· not loaded on this page</small>
                </span>
              )}
              <details>
                <summary>
                  Evidence ({evidenceIds.length}{" "}
                  {evidenceIds.length === 1 ? "record" : "records"})
                </summary>
                <ul>
                  {evidenceIds.map((id) => (
                    <li key={id}>
                      <code>{id}</code>
                    </li>
                  ))}
                </ul>
              </details>
            </li>
          ))}
      </ul>
      {relations.length > limit && (
        <Button variant="outline" onClick={() => setLimit(limit + 100)}>
          Show more relationships ({relations.length - limit} remaining)
        </Button>
      )}
    </>
  );
}

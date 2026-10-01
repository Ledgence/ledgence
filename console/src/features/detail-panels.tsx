// SPDX-License-Identifier: MIT
import { useEffect, useId, useState, type ReactNode } from "react";
import { useLocation, useSearchParams } from "react-router";
import { ChevronDown } from "lucide-react";
import { Button } from "../components/ui/button";
import { readExplorerState, saveExplorerState } from "../app/navigation-state";
import {
  detailDestination,
  resolveDetailNavigation,
  type DetailKind,
  type DetailTab,
} from "./detail-navigation";
import "../styles/detail.css";

function preferredWorkflowView(): "Graph" | "Trace" {
  try {
    const preference = localStorage.getItem("ledgence-explorer-view-v3");
    if (preference === "graph") return "Graph";
    if (preference === "trace" || preference === "timeline") return "Trace";
  } catch {
    /* Storage can be disabled. */
  }
  return window.matchMedia("(max-width: 768px)").matches ? "Trace" : "Graph";
}

export function DetailPanels({
  kind,
  children,
}: {
  kind: DetailKind;
  children: (navigation: {
    tab: DetailTab;
    section: string | null;
    openSection: (section: string) => void;
  }) => ReactNode;
}) {
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  const [preferredView] = useState(preferredWorkflowView);
  const { tab, section, canonical } = resolveDetailNavigation(
    kind,
    params,
    preferredView,
  );
  const canonicalSearch = canonical.toString();
  const currentSearch = params.toString();
  // Preserve the Explorer's presentation through General and legacy URL
  // replacements, even when its graph is not the visible panel.
  useEffect(() => {
    if (kind === "workflow")
      saveExplorerState(
        location.key,
        readExplorerState(location.key, location.state),
      );
  }, [kind, location.key, location.state]);
  useEffect(() => {
    if (canonicalSearch !== currentSearch) {
      setParams(new URLSearchParams(canonicalSearch), {
        replace: true,
        preventScrollReset: true,
        state: { ...location.state, restoreNavigationKey: location.key },
      });
    }
  }, [canonicalSearch, currentSearch, location.key, location.state, setParams]);
  function change(nextTab: DetailTab, nextSection?: string | null) {
    if (kind === "workflow" && nextTab !== "General") {
      try {
        localStorage.setItem(
          "ledgence-explorer-view-v3",
          nextTab.toLowerCase(),
        );
      } catch {
        /* Storage can be disabled. */
      }
    }
    setParams(detailDestination(params, nextTab, nextSection), {
      preventScrollReset: true,
      state: { ...location.state, restoreNavigationKey: location.key },
    });
  }
  return (
    <>
      <nav className="tabs detail-tabs" aria-label="Detail views">
        {(kind === "workflow"
          ? (["Graph", "Trace", "General"] as const)
          : (["Trace", "General"] as const)
        ).map((value) => (
          <Button
            key={value}
            variant="ghost"
            aria-current={value === tab ? "page" : undefined}
            onClick={() => change(value)}
          >
            {value}
          </Button>
        ))}
      </nav>
      {children({
        tab,
        section,
        openSection: (value) => change(tab, value === section ? "none" : value),
      })}
    </>
  );
}

export function DetailSections({
  sections,
  current,
  onChange,
}: {
  sections: {
    id: string;
    title: string;
    description: string;
    content: ReactNode;
  }[];
  current: string | null;
  onChange: (section: string) => void;
}) {
  const id = useId();
  return (
    <div className="detail-sections">
      {sections.map((section) => {
        const open = section.id === current;
        const control = `${id}-${section.id}`;
        return (
          <section key={section.id} className="detail-section" data-open={open}>
            <h2 className="detail-section-heading">
              <button
                id={`${control}-trigger`}
                type="button"
                aria-expanded={open}
                aria-controls={control}
                aria-labelledby={`${control}-label`}
                aria-describedby={`${control}-description`}
                onClick={() => onChange(section.id)}
              >
                <span>
                  <span
                    id={`${control}-label`}
                    className="detail-section-title"
                  >
                    {section.title}
                  </span>
                  <span
                    id={`${control}-description`}
                    className="detail-section-description"
                  >
                    {section.description}
                  </span>
                </span>
                <ChevronDown aria-hidden="true" />
              </button>
            </h2>
            <div
              id={control}
              hidden={!open}
              aria-labelledby={`${control}-trigger`}
              className="detail-section-content"
            >
              {open && section.content}
            </div>
          </section>
        );
      })}
    </div>
  );
}

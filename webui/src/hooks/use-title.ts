import { createContext, useContext, useEffect } from "react";

export const ActivePage = createContext(true);

export function useTitle(title: string) {
  const active = useContext(ActivePage);
  useEffect(() => {
    if (active) document.title = `ZeroFS - ${title}`;
  }, [active, title]);
}

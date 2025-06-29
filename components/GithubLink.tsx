import React from "react";
import { FaGithub } from "react-icons/fa";

interface GithubLinkProps {
  className?: string;
  size: number
  style?: React.CSSProperties;
}

export function GithubLink ({ className, size, style }: GithubLinkProps) {
  return <a
    className={className}
    style={{ ...style, width: size, height: size }}
    href={'https://github.com/datafusion-contrib/datafusion-fiddle'}
    target={'_blank'}
    rel="noopener noreferrer"
  >
    <FaGithub size={size} color="white" />
  </a>
}
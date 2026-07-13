import Icon, { IconName } from "./Icon";

interface EmptyStateProps {
  icon?: IconName;
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
}

export default function EmptyState({
  icon = "inbox",
  title,
  description,
  action,
  className = "",
}: EmptyStateProps) {
  return (
    <div className={`empty-state ${className}`}>
      <Icon name={icon} className="empty-state-icon" aria-hidden={true} />
      <p className="empty-state-title">{title}</p>
      {description && <p className="empty-state-text">{description}</p>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}

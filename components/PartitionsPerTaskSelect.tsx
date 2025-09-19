import React from 'react';
import * as Select from '@radix-ui/react-select';
import { ChevronDownIcon, ChevronUpIcon } from '@radix-ui/react-icons';

export interface PartitionsPerTaskSelectProps {
  partitionsPerTask: number;
  maxPartitions: number;
  onChange: (partitionsPerTask: number) => void;
}

export function PartitionsPerTaskSelect({ partitionsPerTask = 1, maxPartitions = 1, onChange }: PartitionsPerTaskSelectProps) {
  const options = Array.from({ length: maxPartitions }, (_, i) => i + 1);

  return (
    <div className="flex items-center gap-2">
      <label htmlFor="partitions-per-task-select" className="text-sm text-text-secondary">
        Per Task
      </label>
      <Select.Root value={partitionsPerTask?.toString() || '1'} onValueChange={(value) => onChange(parseInt(value))}>
        <Select.Trigger
          id="partitions-per-task-select"
          className="inline-flex items-center justify-between gap-1 px-3 py-1 bg-secondary-surface text-sm text-text-primary rounded border border-gray-600 hover:border-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 cursor-pointer min-w-[60px]"
        >
          <Select.Value />
          <Select.Icon>
            <ChevronDownIcon />
          </Select.Icon>
        </Select.Trigger>
        <Select.Portal>
          <Select.Content className="overflow-hidden bg-secondary-surface rounded-md shadow-lg border border-gray-600 z-50">
            <Select.ScrollUpButton className="flex items-center justify-center h-6 bg-secondary-surface text-text-secondary cursor-default">
              <ChevronUpIcon />
            </Select.ScrollUpButton>
            <Select.Viewport className="p-1">
              {options.map((num) => (
                <Select.Item
                  key={num}
                  value={num.toString()}
                  className="text-sm text-text-primary rounded-sm flex items-center px-6 py-2 relative select-none hover:bg-blue-600 focus:bg-blue-600 cursor-pointer outline-none"
                >
                  <Select.ItemText>{num}</Select.ItemText>
                </Select.Item>
              ))}
            </Select.Viewport>
            <Select.ScrollDownButton className="flex items-center justify-center h-6 bg-secondary-surface text-text-secondary cursor-default">
              <ChevronDownIcon />
            </Select.ScrollDownButton>
          </Select.Content>
        </Select.Portal>
      </Select.Root>
    </div>
  );
}
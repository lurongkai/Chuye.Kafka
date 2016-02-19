using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;

namespace Chuye.Kafka.Utils {
    public class ModelError {
        public String Key { get; set; }
        public String ExceptionMessage { get; set; }
    }

    public static class DataAnnotationHelper {
        private static ICustomTypeDescriptor GetTypeDescriptor(Type type) {
            return new AssociatedMetadataTypeTypeDescriptionProvider(type).GetTypeDescriptor(type);
        }

        public static IEnumerable<ModelError> IsValid(Object value) {
            return from prop in TypeDescriptor.GetProperties(value).Cast<PropertyDescriptor>()
                   from attr in prop.Attributes.OfType<ValidationAttribute>()
                   where !attr.IsValid(prop.GetValue(value))
                   select new ModelError() {
                       Key = prop.Name,
                       ExceptionMessage = attr.FormatErrorMessage(prop.DisplayName)
                   };
        }

        public static void ThrowIfInvalid(Object value) {
            if (value == null) {
                throw new ArgumentNullException("value");
            }
            var modelErrors = IsValid(value);
            if (modelErrors.Any()) {
                throw new Exception(String.Join("; ", modelErrors.Select(me => me.ExceptionMessage)));
            }
        }
    }
}
